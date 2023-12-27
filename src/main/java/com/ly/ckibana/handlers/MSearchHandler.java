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
import com.ly.ckibana.configure.thread.ThreadPoolConfigurer;
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.model.exception.CKNotSupportException;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.parser.MsearchParamParser;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.JSONUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

@Component
public class MSearchHandler extends BaseHandler {

    @Resource
    private ThreadPoolConfigurer threadPoolConfigurer;

    @Resource
    private MsearchParamParser msearchParamParser;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/_msearch").methods(HttpMethod.GET, HttpMethod.POST),
                HttpRoute.newRoute().path("/{index}/_msearch").methods(HttpMethod.GET, HttpMethod.POST),
                HttpRoute.newRoute().path("/{index}/{type}/_msearch").methods(HttpMethod.GET, HttpMethod.POST)
        );
    }

    @Override
    public String doHandle(RequestContext context) throws Exception {
        List<Response> responses = new ArrayList<>();
        String index = context.getIndex();
        List<Callable<Response>> subCkRequests = new ArrayList<>();
        StringBuilder subEsRequest = new StringBuilder();
        // 解析请求，根据索引将查询分为ck和es
        msearchParamParser.parse(context, index, subEsRequest, subCkRequests, responses);
        // 如果es请求不为空，且ck请求为空，直接请求 es，避免序列化开销
        if (StringUtils.isNotEmpty(subEsRequest.toString()) && subCkRequests.isEmpty()) {
            return EsClientUtil.doRequest(context);
        }
        // 查询ck
        execute(subCkRequests, responses);
        // 查询es
        if (!subEsRequest.isEmpty()) {
            String responseBody = EsClientUtil.doRequest(context);
            // merge ck and es response
            JSON.parseObject(responseBody).getJSONArray("responses").forEach(each -> {
                responses.add(JSON.parseObject(each.toString(), Response.class));
            });
        }
        return JSONUtils.serialize(Map.of("responses", responses));
    }

    private void execute(List<Callable<Response>> threadList, List<Response> responses) throws Exception {
        List<Future<Response>> resultList = threadPoolConfigurer.getMSearchExecutor().invokeAll(threadList);
        for (Future<Response> each : resultList) {
            Response response = getThreadResult(each);
            if (response != null) {
                responses.add(response);
            }
        }
    }

    private Response getThreadResult(Future<Response> response) throws Exception {
        Response result;
        try {
            result = response.get();
        } catch (Exception ex) {
            if (ex.getMessage().contains(CKNotSupportException.class.getName())) {
                throw new CKNotSupportException(ex.getMessage().replace(CKNotSupportException.class.getName(), ""));
            } else {
                log.error("getThreadResult", ex);
                throw ex;
            }
        }
        return result;
    }
}
