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
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.JSONUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class ResolveIndexHandler extends BaseHandler {

    @Resource
    private CkService ckService;

    @Override
    public List<HttpRoute> routes() {
        return List.of(HttpRoute.newRoute().path("/_resolve/index/{index}").methods(HttpMethod.GET));
    }

    @Override
    protected String doHandle(RequestContext context) throws Exception {
        List<Map<String, Object>> ckIndices = new ArrayList<>(resolveCkIndices(context));
        String esResponseBody = EsClientUtil.doRequest(context);
        JSON.parseObject(esResponseBody).getJSONArray("indices").forEach(each -> {
            //noinspection unchecked
            ckIndices.add(JSON.parseObject(each.toString(), Map.class));
        });
        return JSONUtils.serialize(Map.of("indices", ckIndices));
    }

    private List<Map<String, Object>> resolveCkIndices(RequestContext context) throws Exception {
        String index = context.getIndex();
        List<String> tableNames;
        if (StringUtils.isEmpty(index) || Objects.equals(index, "*")) {
            tableNames = ckService.queryAllTables(context.getProxyConfig());
        } else {
            List<String> indexNameList = Arrays.stream(StringUtils.split(index, ","))
                    .filter(StringUtils::isNotEmpty).map(StringUtils::trim)
                    .toList();
            tableNames = ckService.queryTables(context.getProxyConfig(), indexNameList);
        }
        return tableNames.stream()
                .filter(tableName -> !context.getProxyConfig().isDirectToEs(tableName))
                .map(tableName -> Map.of("name", tableName, "attributes", List.of("open")))
                .toList();
    }
}
