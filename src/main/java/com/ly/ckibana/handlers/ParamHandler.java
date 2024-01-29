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
package com.ly.ckibana.handlers;

import com.alibaba.fastjson2.JSON;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.model.property.CkProperty;
import com.ly.ckibana.model.property.EsProperty;
import com.ly.ckibana.model.property.KibanaProperty;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ParamHandler extends BaseHandler {
    
    @Resource
    private ProxyConfigLoader proxyConfigLoader;
    
    private static final String CK = "/config/updateCk";
    
    private static final String ES = "/config/updateEs";
    
    private static final String WHITE_INDEX_LIST = "/config/updateWhiteIndexList";
    
    private static final String BLACK_INDEX_LIST = "/config/updateBlackIndexList";
    
    private static final String SAMPLE_LIST = "/config/updateSampleIndexList";
    
    private static final String SAMPLE_MAX_THRESHOLD = "/config/updateSampleCountMaxThreshold";
    
    private static final String USE_CACHE = "/config/updateUseCache";
    
    private static final String ROUND_ABLE_MIN_PERIOD = "/config/updateRoundAbleMinPeriod";
    
    private static final String ROUND = "/config/updateRound";
    
    private static final String MAX_TIME_RANGE = "/config/updateMaxTimeRange";
    
    private static final String ENABLE_MONITORING = "/config/updateEnableMonitoring";
    
    private static final String MSEARCH_THREAD_POOL_CORE_SIZE = "/config/updateMsearchThreadPoolCoreSize";
    
    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path(CK).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(ES).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(WHITE_INDEX_LIST).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(BLACK_INDEX_LIST).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(SAMPLE_LIST).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(SAMPLE_MAX_THRESHOLD).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(USE_CACHE).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(ROUND_ABLE_MIN_PERIOD).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(ROUND).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(MAX_TIME_RANGE).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(ENABLE_MONITORING).methods(HttpMethod.POST),
                HttpRoute.newRoute().path(MSEARCH_THREAD_POOL_CORE_SIZE).methods(HttpMethod.POST)
        );
    }
    
    @Override
    public String doHandle(RequestContext context) {
        
        try {
            Map<String, String> params = context.getRequestInfo().getParams();
            if (params.isEmpty()) {
                return JSONUtils.serialize(Map.of("responses", "未配置参数"));
            }
    
            KibanaProperty kibanaProperty = proxyConfigLoader.getKibanaProperty();
            boolean updateCk = false;
            boolean updateEs = false;
    
            switch (context.getRequestUrl()) {
                case CK:
                    updateCk = updateCK(kibanaProperty, params);
                    break;
                case ES:
                    updateEs = updateES(kibanaProperty, params);
                    break;
                case WHITE_INDEX_LIST:
                    kibanaProperty.getProxy().setWhiteIndexList(parseList(params));
                    break;
                case BLACK_INDEX_LIST:
                    kibanaProperty.getProxy().setBlackIndexList(parseList(params));
                    break;
                case SAMPLE_LIST:
                    kibanaProperty.getQuery().setSampleIndexPatterns(parseList(params));
                    break;
                case SAMPLE_MAX_THRESHOLD:
                    kibanaProperty.getQuery().setSampleCountMaxThreshold(parseValue(params.get("sampleCountMaxThreshold")).intValue());
                    break;
                case USE_CACHE:
                    kibanaProperty.getQuery().setUseCache(parseBoolean(params.get("useCache")));
                    break;
                case ROUND_ABLE_MIN_PERIOD:
                    kibanaProperty.getProxy().setRoundAbleMinPeriod(parseValue(params.get("roundAbleMinPeriod")));
                    break;
                case ROUND:
                    kibanaProperty.getProxy().setRound(parseValue(params.get("round")).intValue());
                    break;
                case MAX_TIME_RANGE:
                    kibanaProperty.getProxy().setMaxTimeRange(parseValue(params.get("maxTimeRange")));
                    break;
                case ENABLE_MONITORING:
                    kibanaProperty.getProxy().setEnableMonitoring(parseBoolean(params.get("enableMonitoring")));
                    break;
                case MSEARCH_THREAD_POOL_CORE_SIZE:
                    kibanaProperty.getThreadPool().getMsearchProperty().setCoreSize(parseValue(params.get("msearchThreadPoolCoreSize")).intValue());
                    break;
                default:
                    return "";
            }
    
            String body = proxyConfigLoader.getYaml().dumpAs(kibanaProperty, Tag.MAP, DumperOptions.FlowStyle.BLOCK);
            String response = EsClientUtil.saveOne(proxyConfigLoader.getMetadataRestClient(), proxyConfigLoader.getSettingsIndexName(),
                    "kibana",
                    Map.of(
                            "key", "kibana",
                            "value", body
                    ),
                    proxyConfigLoader.getMajorVersion());
            //让定时任务自动更新，重新创建ckDatasource、restClient
            if (updateCk) {
                kibanaProperty.getProxy().setCk(null);
            }
            if (updateEs) {
                kibanaProperty.getProxy().setEs(null);
            }
            log.info("update settings:[{}], body:{}, response:[{}]", proxyConfigLoader.getSettingsIndexName(), body, response);
            return response;
        } catch (Exception e) {
            return ProxyUtils.getErrorResponse(e);
        }
    }
    
    private boolean updateCK(KibanaProperty kibanaProperty, Map<String, String> params) {
        String jsonStr = JSON.toJSONString(params);
        CkProperty ckProperty = JSON.parseObject(jsonStr, CkProperty.class);
        if (StringUtils.isBlank(ckProperty.getUrl())
                || StringUtils.isBlank(ckProperty.getUser())
                || StringUtils.isBlank(ckProperty.getPass())
                || StringUtils.isBlank(ckProperty.getDefaultCkDatabase())) {
            throw new IllegalArgumentException("invalid parameter");
        }
        kibanaProperty.getProxy().setCk(ckProperty);
        return true;
    }
    
    private boolean updateES(KibanaProperty kibanaProperty, Map<String, String> params) {
        String host = params.get("host");
        String headersString = params.get("headers");
        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("invalid parameter");
        }
        Map<String, String> headers = new HashMap<>();
        String[] parts = headersString.split(",");
        for (String part: parts) {
            String[] keyValue = part.split(":");
            headers.put(keyValue[0], keyValue[1]);
        }
        
        EsProperty esProperty = new EsProperty(host, headers);
        kibanaProperty.getProxy().setEs(esProperty);
        return true;
    }
    
    private List<String> parseList(Map<String, String> params) {
        String listParam = params.get("list");
        if (StringUtils.isBlank(listParam)) {
            throw new IllegalArgumentException("invalid parameter");
        }
        String[] parts = listParam.split(",");
        return Arrays.asList(parts);
    }
    
    private Long parseValue(String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("invalid parameter");
        }
        return Long.parseLong(value);
    }
    
    private Boolean parseBoolean(String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("invalid parameter");
        }
        return Boolean.parseBoolean(value);
    }
}
