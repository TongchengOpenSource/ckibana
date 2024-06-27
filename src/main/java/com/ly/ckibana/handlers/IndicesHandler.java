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

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.exception.FallbackToEsException;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.JSONUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * IndicesHandler.
 *
 * @author caojiaqiang
 */
@Component
public class IndicesHandler extends BaseHandler {

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("_cat/indices/{index}").methods(HttpMethod.GET, HttpMethod.POST)
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
            return EsClientUtil.doRequest(context);
        }
        
        JSONArray result = new JSONArray();
        JSONObject object = new JSONObject();
        object.put("health", "green");
        object.put("status", "open");
        object.put("index", index);
        result.add(object);
        return JSONUtils.serialize(result);
    }

}
