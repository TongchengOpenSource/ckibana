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
package com.ly.ckibana.service;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.property.KibanaProperty;
import com.ly.ckibana.model.property.MetadataConfigProperty;
import com.ly.ckibana.model.property.QueryProperty;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.util.RestUtils;
import com.ly.ckibana.util.Utils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 缓存服务.
 */
@Slf4j
@Service
@Data
public class CkResultCacheService {

    private String indexName = Constants.ConfigFile.CACHE_INDEX_NAME;

    private RequestContext requestContext;

    private Integer majorVersion;

    @Resource
    private MetadataConfigProperty metadataConfigProperty;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @PostConstruct
    public void init() {
        if (!proxyConfigLoader.isUtEnv()) {
            requestContext = RestUtils.createRequestContext(metadataConfigProperty.getHosts(), metadataConfigProperty.getHeaders());
            majorVersion = proxyConfigLoader.getMajorVersion();
        }

    }

    public void put(String key, List<JSONObject> value) {
        if (key.contains(SqlConstants.SYSTEM_TABLE) || !proxyConfigLoader.getKibanaProperty().getQuery().isUseCache()) {
            return;
        }
        if (!containsKey(key)) {
            synchronized (this) {
                if (!containsKey(key)) {
                    long startTime = System.currentTimeMillis();
                    long cost = Duration.between(Instant.ofEpochMilli(startTime), Instant.ofEpochMilli(System.currentTimeMillis())).toMillis();
                    EsClientUtil.saveOne(requestContext.getProxyConfig().getRestClient(), Utils.getIndexName(indexName, Constants.DATE_FORMAT_DEFAULT),
                            Utils.toUuid(key), buildBulkBody(key, value), majorVersion);
                    log.info("[cache-add][{}ms] key={}, uuid={}", cost, key, Utils.toUuid(key));
                }
            }
        }
    }

    public List<JSONObject> get(String key) {
        JSONObject sourceObj = EsClientUtil.getSource(requestContext.getProxyConfig().getRestClient(), Utils.getIndexName(indexName, Constants.DATE_FORMAT_DEFAULT), key);
        if (sourceObj != null) {
            return sourceObj.getJSONArray("value").toJavaList(JSONObject.class);
        }
        return null;
    }

    public boolean containsKey(String key) {
        // 如果不使用缓存，则不进行查询
        if (key.contains(SqlConstants.SYSTEM_TABLE) || !proxyConfigLoader.getKibanaProperty().getQuery().isUseCache()) {
            return false;
        }
        Boolean useCache = Optional.ofNullable(proxyConfigLoader.getKibanaProperty())
                .map(KibanaProperty::getQuery)
                .map(QueryProperty::isUseCache)
                .orElse(false);
        return useCache && get(key) != null;
    }

    private Map<String, Object> buildBulkBody(String key, List<JSONObject> value) {
        Map<String, Object> map = new HashMap<>(3);
        map.put("key", key);
        map.put("value", value.toString());
        map.put("timestamp", System.currentTimeMillis());
        return map;
    }

}
