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
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.exception.FallbackToEsException;
import com.ly.ckibana.model.property.MetadataConfigProperty;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.parser.ParamParser;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.JSONUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * MappingHandler.
 *
 * @author caojiaqiang
 */
@Component
public class MappingHandler extends BaseHandler {

    @Resource
    private ParamParser paramParser;

    @Resource
    private MetadataConfigProperty metadataConfigProperty;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/{index}/_mapping").methods(HttpMethod.GET, HttpMethod.POST)
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public String doHandle(RequestContext context) throws Exception {
        String index = context.getIndex();
        // es6.x 查询索引列表时，传入index默认为 * 或 *:*，此时解析请求体，判断是否查询索引列表，如果是的话，就查询ck和es，merge后返回
        if (StringUtils.isEmpty(index)) {
            throw new FallbackToEsException();
        }
        if (!index.equals(Constants.MATCH_ALL) && !context.isCkIndex()) {
            throw new FallbackToEsException();
        }
        
        ProxyConfig proxyConfig = context.getProxyConfig();
        IndexPattern indexPattern = proxyConfig.buildIndexPattern(context.getOriginalIndex(), index);
        indexPattern.setTimeField(EsClientUtil.getIndexPatternMeta(context.getProxyConfig().getRestClient(), metadataConfigProperty.getHeaders()).getOrDefault(index, null));
        Map<String, JSONObject> fields = paramParser.queryIndexPatternFields(context, indexPattern, true);
    
        JSONObject properties = new JSONObject();
        for (Map.Entry<String, JSONObject> entry : fields.entrySet()) {
            JSONObject entryValue = entry.getValue();
            for (String key : entryValue.keySet()) {
                properties.put(entry.getKey(), entryValue.getJSONObject(key));
            }
        }
        
        JSONObject data = new JSONObject();
        data.put("properties", properties);
        data.put("dynamic", false);
        data.put("date_detection", false);
        
        JSONObject info = new JSONObject();
        info.put("info", data);
        
        JSONObject mapping = new JSONObject();
        mapping.put("mappings", info);
    
        JSONObject result = new JSONObject();
        result.put(index, mapping);
        return JSONUtils.serialize(result);
    }

}
