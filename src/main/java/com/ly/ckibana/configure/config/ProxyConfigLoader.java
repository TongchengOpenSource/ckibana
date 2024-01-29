/*
 * Copyright (c) 2023 LY.com All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ly.ckibana.configure.config;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.EnvConstants;
import com.ly.ckibana.model.exception.IndexNotFoundException;
import com.ly.ckibana.model.exception.InitializationException;
import com.ly.ckibana.model.property.KibanaItemProperty;
import com.ly.ckibana.model.property.KibanaProperty;
import com.ly.ckibana.model.property.MetadataConfigProperty;
import com.ly.ckibana.model.property.QueryProperty;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.RestUtils;
import com.ly.ckibana.util.Utils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;

@Component
@Slf4j
public class ProxyConfigLoader {

    private ProxyConfig proxyConfig;

    @Getter
    private RestClient metadataRestClient;

    private String kibanaPropertyMd5;

    private String kibanaPropertyOriginalString;

    @Getter
    @Setter
    private KibanaProperty kibanaProperty;

    @Resource
    @Getter
    private MetadataConfigProperty metadataConfigProperty;

    @Value("${spring.profiles.active:default}")
    @Getter
    private String activeProfileEnv;

    @Getter
    @Setter
    private boolean isUtEnv;

    @Getter
    private Integer majorVersion;

    @Getter
    private Yaml yaml = new Yaml();

    @PostConstruct
    public void init() {
        this.isUtEnv = EnvConstants.ENV_UT.equals(activeProfileEnv);
        if (this.isUtEnv) {
            return;
        }
        initConfig();
        doInit();
    }

    private void doInit() {
        // init from es
        RequestContext requestContext = RestUtils.createRequestContext(metadataConfigProperty.getHosts(), metadataConfigProperty.getHeaders());
        metadataRestClient = requestContext.getProxyConfig().getRestClient();

        refreshConfig();

        // 初始化proxy-settings 索引
        boolean initSettingsIndex = EsClientUtil.createIndex(metadataRestClient, getSettingsIndexName(),
                Constants.ConfigFile.SETTINGS_PROPERTIES, Constants.ConfigFile.SETTINGS_INDEX_SHARDS, majorVersion);
        log.info("[proxy-settings] init [{}] ", initSettingsIndex);

        // 初始化proxy-cache
        boolean initCacheIndex = EsClientUtil.createTemplate(metadataRestClient, Constants.ConfigFile.CACHE_INDEX_NAME,
                Constants.ConfigFile.CACHE_PROPERTIES, Constants.ConfigFile.CACHE_INDEX_SHARDS, majorVersion);
        log.info("[proxy-cache] init [{}] ", initCacheIndex);

        // 初始化黑名单proxy-black-list
        boolean initBlackListIndex = EsClientUtil.createTemplate(metadataRestClient, Constants.ConfigFile.BLACK_LIST_INDEX_NAME,
                Constants.ConfigFile.BLACK_LIST_PROPERTIES, Constants.ConfigFile.BLACK_LIST_INDEX_SHARDS, majorVersion);
        log.info("[proxy-black-list] init [{}] ", initBlackListIndex);

        // 初始化监控记录proxy-monitor
        boolean initMonitorIndex = EsClientUtil.createTemplate(metadataRestClient, Constants.ConfigFile.MONITOR_INDEX_NAME,
                Constants.ConfigFile.MONITOR_PROPERTIES, Constants.ConfigFile.MONITOR_INDEX_SHARDS, majorVersion);
        log.info("[proxy-monitor] init [{}] ", initMonitorIndex);

        if (!initSettingsIndex || !initCacheIndex || !initBlackListIndex || !initMonitorIndex) {
            throw new InitializationException("index init failed");
        }
    }

    private void initConfig() {
        kibanaPropertyMd5 = null;
        kibanaProperty = new KibanaProperty();
        KibanaItemProperty defaultProxyConfig = getDefaultKibanaItemProperty();
        proxyConfig = new ProxyConfig(defaultProxyConfig);
        kibanaProperty.setProxy(defaultProxyConfig);
    }

    public void refreshConfig() {
        // 直到获取成功配置信息
        String searchResult = "";
        try {
            majorVersion = EsClientUtil.getMajorVersion(EsClientUtil.getClusterInfo(metadataRestClient));
            String response = EsClientUtil.getIndexSetting(metadataRestClient, getSettingsIndexName());
            if (StringUtils.isEmpty(response)) {
                log.warn("get index settings {}, response is empty " + getSettingsIndexName());
                return;
            }
            // 获取内容
            String searchQuery = "{\"size\":1,\"query\":{\"term\":{\"key\":{\"value\":\"kibana\"}}}}";
            searchResult = EsClientUtil.search(metadataRestClient, getSettingsIndexName(), searchQuery);
            JSONObject searchResultObj = JSONObject.parseObject(searchResult);
            JSONObject hitObj = searchResultObj.getJSONObject("hits");
            JSONArray hitsDataObjArray = hitObj.getJSONArray("hits");
            if (hitsDataObjArray.isEmpty()) {
                initConfig();
                log.warn("no config in {}. set to default configuration. [\n{}\n]", getSettingsIndexName(), JSON.toJSONString(proxyConfig));
                return;
            }
            JSONObject data = (JSONObject) hitsDataObjArray.get(0);
            String newConfig = data.getJSONObject("_source").getString("value");
            KibanaProperty newKibanaProperty = yaml.loadAs(newConfig, KibanaProperty.class);
            String newKibanaPropertyMd5 = Utils.toUuid(newConfig);
            boolean configChanged = kibanaPropertyMd5 == null || !kibanaPropertyMd5.equals(newKibanaPropertyMd5);
            if (configChanged) {
                // 不填则采用默认配置
                QueryProperty queryProperty = newKibanaProperty.getQuery() == null ? new QueryProperty() : newKibanaProperty.getQuery();
                newKibanaProperty.setQuery(queryProperty);
                log.info("the config {}, reload it. old: [\n{}\n], new: [\n{}\n]", kibanaPropertyMd5 == null ? "init" : "changed", kibanaPropertyOriginalString, newConfig);
                kibanaPropertyMd5 = newKibanaPropertyMd5;
                kibanaPropertyOriginalString = newConfig;
                updateProxyConfigMap(newKibanaProperty);
            }
        } catch (IndexNotFoundException e) {
            EsClientUtil.createIndex(metadataRestClient, getSettingsIndexName(), Constants.ConfigFile.SETTINGS_PROPERTIES, kibanaProperty.getDefaultShard(), majorVersion);
            initConfig();
        }
    }

    public ProxyConfig getConfig() {
        return this.proxyConfig;
    }

    private KibanaItemProperty getDefaultKibanaItemProperty() {
        return new KibanaItemProperty(metadataConfigProperty.getHosts(), metadataConfigProperty.getHeaders());
    }

    public void updateProxyConfigMap(KibanaProperty newKibanaProperty) {
        kibanaProperty.setQuery(newKibanaProperty.getQuery());
        kibanaProperty.setProxy(KibanaItemProperty.buildProxy(newKibanaProperty.getProxy(), this::getDefaultKibanaItemProperty));
        kibanaProperty.setThreadPool(newKibanaProperty.getThreadPool());

        boolean updateEsClient = false;
        boolean updateCkClient = false;
        if (proxyConfig != null) {
            updateEsClient = proxyConfig.shouldUpdateEsClient(this.kibanaProperty.getProxy());
            updateCkClient = proxyConfig.shouldUpdateCkClient(this.kibanaProperty.getProxy());
            proxyConfig.setKibanaItemProperty(this.kibanaProperty.getProxy());
        } else {
            proxyConfig = new ProxyConfig(this.kibanaProperty.getProxy());
        }
        if (updateEsClient) {
            try {
                proxyConfig.getRestClient().close();
                // create a new es client
                proxyConfig.setRestClient(RestUtils.initEsResClient(this.kibanaProperty.getProxy().getEs()));
            } catch (IOException e) {
                log.error("close rest client error.", e);
            }
        }
        if (updateCkClient) {
            proxyConfig.setCkDatasource(CkService.initDatasource(this.kibanaProperty.getProxy().getCk()));
        }
    }

    public String getSettingsIndexName() {
        return Constants.ConfigFile.SETTINGS_INDEX_NAME
               + (StringUtils.isBlank(metadataConfigProperty.getSettingsSuffix()) ? Strings.EMPTY : String.format("-%s", metadataConfigProperty.getSettingsSuffix()));
    }
}
