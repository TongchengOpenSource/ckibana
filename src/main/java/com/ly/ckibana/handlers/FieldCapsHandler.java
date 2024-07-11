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
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.exception.FallbackToEsException;
import com.ly.ckibana.model.property.MetadataConfigProperty;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.parser.ParamParser;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.JSONUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class FieldCapsHandler extends BaseHandler {
    
    @Resource
    private ParamParser paramParser;

    @Resource
    private MetadataConfigProperty metadataConfigProperty;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/_field_caps").methods(HttpMethod.GET, HttpMethod.POST),
                HttpRoute.newRoute().path("/{index}/_field_caps").methods(HttpMethod.GET, HttpMethod.POST)
        );
    }

    /**
     * 目前支持query和update代理.
     * 将ck的字段转为kibana使用的es类型，
     * indexpattern元数据仍保存在es
     * 查询索引字段列表
     * https://www.elastic.co/guide/en/elasticsearch/reference/master/search-field-caps.html
     * 额外支持非DateTime类型的字段作为时间字段。具体参见ProxyUtils.isTimeFieldOptionByName()
     */
    @Override
    public String doHandle(RequestContext context) throws Exception {
        if (!context.isCkIndex()) {
            throw new FallbackToEsException();
        }
        String index = context.getIndex();
        JSONObject result = new JSONObject();
        ProxyConfig proxyConfig = context.getProxyConfig();
        IndexPattern indexPattern = proxyConfig.buildIndexPattern(context.getOriginalIndex(), index);
        indexPattern.setTimeField(EsClientUtil.getIndexPatternMeta(context.getProxyConfig().getRestClient(), metadataConfigProperty.getHeaders()).getOrDefault(index, null));
        Map<String, JSONObject> fields = paramParser.queryIndexPatternFields(context, indexPattern, true);
        result.put("fields", fields);
        return JSONUtils.serialize(result);
    }
}
