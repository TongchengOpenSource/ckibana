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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.ExceptionConstants;
import com.ly.ckibana.model.exception.IndexNotFoundException;
import com.ly.ckibana.model.exception.InvalidClusterInfoException;
import com.ly.ckibana.model.exception.InvalidIndexSettingsException;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.Utils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.util.CollectionUtils;

import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class EsClientUtil {
    public static final Logger log = LoggerFactory.getLogger(EsClientUtil.class);

    protected static final Header[] BASE_HEADER = new Header[]{new BasicHeader("Content-Type", "application/json")};

    /**
     * 不指定则采用用户restclient.
     */
    public static String doRequest(RequestContext context) throws Exception {
        return doRequest(context.getProxyConfig().getUserRestClient(), context.getRequestInfo(), context.getHttpResponse());
    }

    public static String doRequest(RestClient restClient, RequestContext.RequestInfo requestInfo, HttpServletResponse response) throws Exception {
        return doRequest(restClient, requestInfo.getMethod(), requestInfo.getUrl(), requestInfo.getHeaders(), requestInfo.getParams(), requestInfo.getRequestBody(), response);
    }

    /**
     * 发起es请求，并返回代理结果.
     */
    public static String doRequest(RestClient restClient, String method, String uri, Header[] headers, Map<String, String> params,
                                   String requestBody, HttpServletResponse httpResponse) throws Exception {
        HttpEntity entity = null;
        if (!StringUtils.isEmpty(requestBody)) {
            entity = new NStringEntity(requestBody, StandardCharsets.UTF_8);
        }
        String result;
        try {
            Response response = performRequest(restClient, method, uri, entity, headers, params);
            RESPONSE_CONSUMER.accept(response, httpResponse);
            result = response.getEntity() == null ? "" : IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
        } catch (Exception ex) {
            log.error("requestEs error, uri={}, requestBody={}, params={}", uri, requestBody, JSONUtils.serialize(params), ex);
            throw ex;
        }
        return result;
    }

    public static String doRequest(RestClient restClient, String method, String uri, Header[] headers, Map<String, String> params, String requestBody) throws Exception {
        return doRequest(restClient, method, uri, headers, params, requestBody, null);
    }

    private static Response performRequest(RestClient readClient, String method, String uri, HttpEntity entity, Header[] basicHeaders, Map<String, String> params) throws Exception {
        Request request = new Request(method, uri);
        request.setEntity(entity);
        request.setOptions(buildRequestOptions(basicHeaders));
        Response result;
        if (params != null) {
            request.addParameters(params);
        }
        try {
            result = readClient.performRequest(request);
        } catch (ResponseException ex) {
            result = ex.getResponse();
        } catch (Exception ex) {
            log.error("performRequest error, uri={}, params={}", uri, JSONUtils.serialize(params), ex);
            throw ex;
        }
        return result;
    }

    private static RequestOptions buildRequestOptions(Header[] basicHeaders) {
        RequestOptions.Builder requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
        if (basicHeaders == null) {
            return requestOptionsBuilder.build();
        }
        for (Header header : basicHeaders) {
            requestOptionsBuilder.addHeader(header.getName(), header.getValue());
        }
        return requestOptionsBuilder.build();
    }

    public static String getIndexSetting(RestClient restClient, String indexName) throws IndexNotFoundException {
        String response;
        try {
            response = doRequest(restClient, HttpMethod.GET.name(), String.format("/%s/_settings", indexName), BASE_HEADER, null, "");
        } catch (Exception e) {
            log.error("get index settings error, indexName:{}", indexName, e);
            throw new InvalidIndexSettingsException("invalid index settings, index: " + indexName);
        }
        if (StringUtils.isEmpty(response)) {
            log.warn("es response is empty. [{}]. ", indexName);
            throw new InvalidIndexSettingsException("invalid index settings, index: " + indexName);
        }
        if (response.contains(ExceptionConstants.INDEX_NOT_FOUND_EXCEPTION)) {
            throw new IndexNotFoundException(indexName + " not found");
        }
        return response;
    }

    public static String getClusterInfo(RestClient restClient) {
        String response;
        try {
            response = doRequest(restClient, HttpMethod.GET.name(), "/", BASE_HEADER, null, "");
        } catch (Exception e) {
            log.error("Failed to get cluster information. path : /", e);
            throw new InvalidClusterInfoException("failed to get cluster information.");
        }
        if (response.contains("error")) {
            log.error("Failed to get cluster information. path : /, response: {}", response);
            throw new InvalidClusterInfoException("failed to get cluster information.");
        }
        return response;
    }

    public static String bulk(RestClient restClient, String body) {
        String response = null;
        try {
            response = doRequest(restClient, HttpMethod.PUT.name(), "/_bulk", BASE_HEADER, null, body);
        } catch (Exception e) {
            log.error("bulk error", e);
        }
        return response;
    }

    public static String search(RestClient restClient, String indexName, String body) {
        String response = null;
        try {
            response = doRequest(restClient, HttpMethod.GET.name(), String.format("/%s/_search", indexName), BASE_HEADER, null, body);
        } catch (Exception e) {
            log.error("search error, indexName:{}", indexName, e);
        }
        return response;
    }

    public static String search(RestClient restClient, String indexName, Map<String, String> paramsMap, String body) {
        String response = null;
        try {
            response = doRequest(restClient, HttpMethod.GET.name(), String.format("/%s/_search", indexName), BASE_HEADER, paramsMap, body);
        } catch (Exception e) {
            log.error("search error", e);
        }
        return response;
    }

    public static boolean createIndex(RestClient restClient, String indexName, Map<String, Object> fieldTypeMap, int shard, int majorVersion) {
        String properties = String.format("\"properties\":%s", JSONObject.toJSONString(fieldTypeMap));
        String defaultSettings = buildDefaultSettingsAndProperties(majorVersion, shard, "", properties);
        String response;
        try {
            response = doRequest(restClient, HttpMethod.PUT.name(), String.format("/%s", indexName), BASE_HEADER, null, defaultSettings);
        } catch (Exception e) {
            log.error("create index error", e);
            return false;
        }
        if (response.contains(ExceptionConstants.RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
            log.warn("index already exists. index: {}", indexName);
            return true;
        }
        if (response.contains("error")) {
            log.error("create index error. response: {}", response);
            return false;
        }
        return true;
    }

    public static boolean createTemplate(RestClient restClient, String indexName, Map<String, Object> fieldTypeMap, int shard, int majorVersion) {
        boolean result = true;
        String properties = String.format("\"properties\":%s", JSONObject.toJSONString(fieldTypeMap));
        // 创建模版
        String pattern = String.format("\"order\":0,\"index_patterns\":[\"%s-*\"],", indexName);
        String defaultSettings = buildDefaultSettingsAndProperties(majorVersion, shard, pattern, properties);
        String response = "";
        try {
            response = doRequest(restClient, HttpMethod.PUT.name(), String.format("/_template/%s", indexName), BASE_HEADER, null, defaultSettings);
        } catch (Exception e) {
            log.error("create index template error, indexName:{}, fieldTypeMap:{}, majorVersion:{}", indexName, fieldTypeMap, majorVersion, e);
            result = false;
        }
        if (response.contains("error")) {
            log.error("create index template error. response: {}", response);
            result = false;
        }
        return result;
    }

    public static Integer getMajorVersion(String clusterInfo) {
        if (clusterInfo == null) {
            throw new InvalidClusterInfoException("cluster info is null");
        }
        try {
            JSONObject jsonObject = JSONObject.parseObject(clusterInfo);
            JSONObject versionObj = jsonObject.getJSONObject("version");
            if (versionObj == null) {
                log.error("parse es response error. version is null. response: {}", clusterInfo);
                throw new InvalidClusterInfoException("parse es response error. version is null");
            }
            return Integer.parseInt(versionObj.getString("number").split("\\.")[0]);
        } catch (Exception e) {
            log.error("parse es response error. response: {}", clusterInfo, e);
            throw new InvalidClusterInfoException("parse es response error.");
        }
    }

    public static String buildDefaultSettingsAndProperties(Integer majorVersion, int shard, String extra, String properties) {
        StringBuilder builder = new StringBuilder("{");
        StringBuilder endBuilder = new StringBuilder("}");
        builder.append(extra);
        builder.append("\"settings\": ")
                .append("{   \"index\": {"
                        + "       \"codec\": \"best_compression\","
                        + "       \"number_of_shards\": " + shard + ","
                        + "       \"translog\": {\"durability\": \"async\"}"
                        + "   }"
                        + "},")
                .append("\"mappings\": {");
        if (majorVersion < 7) {
            builder.append("\"info\": {");
            if (majorVersion < 6) {
                builder.append("\"_all\": {\"enabled\": false},");
            }
            endBuilder.append("}");
        }
        builder.append("\"dynamic\": false,"
                       + "\"date_detection\": false,")
                .append(properties);

        builder.append("}").append(endBuilder);
        return builder.toString();
    }

    public static JSONObject getSource(RestClient restClient, String indexName, String key) {
        String response = "";
        try {
            response = doRequest(restClient, HttpMethod.GET.name(),
                    String.format("/%s/_doc/%s", indexName, Utils.toUuid(key)), BASE_HEADER, null, "");
            if (StringUtils.isEmpty(response)) {
                return null;
            }
            // 获取内容
            JSONObject responseObj = JSONObject.parseObject(response);
            return responseObj.getJSONObject("_source");
        } catch (Exception e) {
            log.error("get cache error. key:{}, response:{}", key, response, e);
        }
        return null;
    }

    public static String deleteSource(RestClient restClient, String indexName, String id) {
        String response = "";
        try {
            response = doRequest(restClient, HttpMethod.DELETE.name(),
                    String.format("/%s/_doc/%s", indexName, id), BASE_HEADER, null, "");
            if (StringUtils.isEmpty(response)) {
                return null;
            }
            // 获取内容
            return response;
        } catch (Exception e) {
            log.error("delete data error. indexName:{} id:{}, response:{}", indexName, id, response, e);
        }
        return null;
    }

    public static String saveOne(RestClient restClient, String indexName,
                                 String id, Map<String, Object> map, Integer majorVersion) {
        String curIndexFormat = Constants.IndexBuilder.BULK_INDEX_HEADER;
        if (majorVersion > 6) {
            curIndexFormat = Constants.IndexBuilder.BULK_INDEX_NO_TYPE_HEADER;
        }
        String bulkBody = String.format(curIndexFormat, indexName, id)
                          + "\n"
                          + JSON.toJSONString(map)
                          + "\n";
        // save
        String response = bulk(restClient, bulkBody);
        if (StringUtils.isEmpty(response) || response.contains("\"errors\":true") || response.contains("\"error\":")) {
            log.error("save data error. id:{}, uuid:{}, response:{}", id, Utils.toUuid(id), response);
        }
        return response;
    }

    public static String saveBatch(RestClient restClient, String indexName,
                                   List<Map<String, Object>> dataList, Integer majorVersion) {
        String curIndexFormat = Constants.IndexBuilder.BULK_INDEX_HEADER;
        if (majorVersion > 6) {
            curIndexFormat = Constants.IndexBuilder.BULK_INDEX_NO_TYPE_HEADER;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (Map<String, Object> data : dataList) {
            String id = data.containsKey("id") ? (String) data.get("id") : Utils.getRandomUuid();
            stringBuilder.append(String.format(curIndexFormat, indexName, id))
                    .append("\n")
                    .append(JSON.toJSONString(data))
                    .append("\n");
            data.put("id", id);
        }

        // save
        String response = bulk(restClient, stringBuilder.toString());
        if (StringUtils.isEmpty(response) || response.contains("\"errors\":true") || response.contains("\"error\":")) {
            log.error("save data error. response:{}", response);
        }
        return response;
    }

    /**
     * 当前代理的indexPattern元数据.
     * 获取kibana元数据，每个indexpattern的时间字段名称，用于msearch查询
     *
     * @param restClient proxy request client
     * @return 元数据
     */
    public static Map<String, String> getIndexPatternMeta(RestClient restClient, Map<String, String> headers) {
        Map<String, String> result = new HashMap<>();
        try {
            Header[] queryHeaders;
            if (!CollectionUtils.isEmpty(headers)) {
                queryHeaders = new Header[headers.size() + 1];
                int index = 0;
                for (Map.Entry<String, String> item : headers.entrySet()) {
                    queryHeaders[++index] = new BasicHeader(item.getKey(), item.getValue());
                }
                queryHeaders[0] = new BasicHeader("Content-Type", "application/json");
            } else {
                queryHeaders = BASE_HEADER;
            }

            String query = "{\"size\":10000,\"seq_no_primary_term\":true,\"query\":{\"bool\":{\"filter\":[{\"bool\":{\"should\":"
                           + "[{\"bool\":{\"must\":[{\"term\":{\"type\":\"index-pattern\"}}],\"must_not\":[{\"exists\":{\"field\":\"namespace\"}}]}}],\"minimum_should_match\":1}}]}}}";
            String responseBody = doRequest(restClient,
                    HttpMethod.POST.name(), "/" + Constants.KIBANA_META_INDEX + "/_search?ignore_unavailable=true",
                    queryHeaders, null, query);
            JSONObject responseObj = JSONObject.parse(responseBody);
            if (responseObj.containsKey(Constants.HIT) && responseObj.getJSONObject(Constants.HIT).containsKey(Constants.HIT)) {
                JSONArray array = responseObj.getJSONObject(Constants.HIT).getJSONArray(Constants.HIT);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject item = array.getJSONObject(i).getJSONObject(Constants.SOURCE).getJSONObject(Constants.INDEX_PATTERN);
                    String key = item.getString("title");
                    String timeField = item.getString("timeFieldName");
                    result.put(key, timeField);
                }
            }
        } catch (Exception ex) {
            log.error("getIndexPatternEsMeta", ex);
        }
        return result;
    }

    public static final BiConsumer<Response, HttpServletResponse> RESPONSE_CONSUMER = (esResponse, httpServletResponse) -> {
        if (esResponse == null || httpServletResponse == null) {
            return;
        }
        Header[] esResponseHeaders = esResponse.getHeaders();
        for (Header esResponseHeader : esResponseHeaders) {
            if (HttpHeaders.CONTENT_LENGTH.equalsIgnoreCase(esResponseHeader.getName()) || HttpHeaders.TRANSFER_ENCODING.equalsIgnoreCase(esResponseHeader.getName())) {
                continue;
            }
            if (HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(esResponseHeader.getName())) {
                httpServletResponse.setContentType(esResponseHeader.getValue());
            } else {
                httpServletResponse.setHeader(esResponseHeader.getName(), esResponseHeader.getValue());
            }
        }
        // 设置code码
        httpServletResponse.setStatus(esResponse.getStatusLine().getStatusCode());
    };
}
