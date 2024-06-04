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
import com.ly.ckibana.model.property.MetadataConfigProperty;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.util.RestUtils;
import com.ly.ckibana.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * 黑名单.
 */
@Slf4j
@Service
public class BlackSqlService {
    private static final String INDEX_NAME = Constants.ConfigFile.BLACK_LIST_INDEX_NAME;

    private RequestContext requestContext;

    @Resource
    private MetadataConfigProperty metadataConfigProperty;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @PostConstruct
    public void init() {
        if (proxyConfigLoader.isUtEnv()) {
            return;
        }
        requestContext = RestUtils.createRequestContext(metadataConfigProperty.getHosts(), metadataConfigProperty.getHeaders());
    }

    public String addBlackSql(long range, String sql) {
        String key = buildKey(range, sql);
        log.info("add to black-list index: {}", key);
        return EsClientUtil.saveOne(requestContext.getProxyConfig().getRestClient(), INDEX_NAME, Utils.toUuid(key), buildBulkBody(key), proxyConfigLoader.getMajorVersion());
    }

    public String removeBlackSql(String id) {
        return EsClientUtil.deleteSource(requestContext.getProxyConfig().getRestClient(), Constants.ConfigFile.BLACK_LIST_INDEX_NAME, id);
    }

    public String getList(int size) {
        return EsClientUtil.search(requestContext.getProxyConfig().getRestClient(), Constants.ConfigFile.BLACK_LIST_INDEX_NAME, String.format("{\"size\":%s}", size));
    }

    public boolean isBlackSql(long range, String sql) {
        JSONObject sourceObj = EsClientUtil.getSource(requestContext.getProxyConfig().getRestClient(), INDEX_NAME, buildKey(range, sql));
        return sourceObj != null;
    }

    private String buildKey(long range, String sql) {
        return range + "_" + sql;
    }

    private Map<String, Object> buildBulkBody(String key) {
        Map<String, Object> map = new HashMap<>(3);
        map.put("key", key);
        map.put("timestamp", System.currentTimeMillis());
        return map;
    }
}
