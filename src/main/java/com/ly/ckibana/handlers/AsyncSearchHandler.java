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

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.exception.FallbackToEsException;
import com.ly.ckibana.model.exception.UiException;
import com.ly.ckibana.model.property.MetadataConfigProperty;
import com.ly.ckibana.model.property.QueryProperty;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.parser.AggResultParser;
import com.ly.ckibana.parser.MsearchParamParser;
import com.ly.ckibana.parser.ParamParser;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class AsyncSearchHandler extends BaseHandler {

    @Resource
    private ParamParser paramParser;

    @Resource
    private AggResultParser aggsParser;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Resource
    private MsearchParamParser msearchParamParser;

    @Resource
    private MetadataConfigProperty metadataConfigProperty;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/_async_search").methods(HttpMethod.GET, HttpMethod.POST),
                HttpRoute.newRoute().path("/{index}/_async_search").methods(HttpMethod.GET, HttpMethod.POST)
        );
    }

    @Override
    public String doHandle(RequestContext context) throws Exception {
        String index = context.getIndex();
        if (!context.isCkIndex()) {
            throw new FallbackToEsException();
        }
        ProxyConfig proxyConfig = context.getProxyConfig();
        JSONObject searchQuery = JSONUtils.deserialize(context.getRequestInfo().getRequestBody(), JSONObject.class);
        IndexPattern indexPattern = proxyConfig.buildIndexPattern(index);
        CkRequestContext ckRequestContext = new CkRequestContext(context.getClientIp(), indexPattern, paramParser.getMaxResultRow());
        Map<String, Map<String, String>> tableColumnsCache = new HashMap<>();
        String timeField = EsClientUtil.getIndexPatternMeta(context.getProxyConfig().getRestClient(), metadataConfigProperty.getHeaders()).get(index);
        if (timeField == null) {
            log.warn("please set the date field of this index. [{}]", index);
            return JSONUtils.serialize(ProxyUtils.newKibanaException("请设置该索引的date字段"));
        }
        QueryProperty queryProperty = proxyConfigLoader.getKibanaProperty().getQuery();
        if (paramParser.checkIfNeedSampleByIndex(index)) {
            CkRequestContext.SampleParam sampleParam = new CkRequestContext.SampleParam(Constants.USE_SAMPLE_COUNT_THREASHOLD, queryProperty.getSampleCountMaxThreshold());
            long totalCount = aggsParser.queryTotalCount(ckRequestContext, null);
            sampleParam.setSampleTotalCount(totalCount);
            ckRequestContext.setSampleParam(sampleParam);
        }
        try {
            msearchParamParser.parseRequestBySearchQuery(tableColumnsCache, searchQuery, timeField, indexPattern, ckRequestContext);
            Response response = aggsParser.buildMSearchAggResult(ckRequestContext);
            return JSONUtils.serialize(Map.of("response", response));
        } catch (UiException e) {
            context.getHttpResponse().setStatus(HttpStatus.BAD_REQUEST.value());
            return JSONUtils.serialize(ProxyUtils.newKibanaExceptionV8(e.getUiShow()));
        }
    }
}
