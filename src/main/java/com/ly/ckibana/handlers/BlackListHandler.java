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
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.service.BlackSqlService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class BlackListHandler extends BaseHandler {

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Resource
    private BlackSqlService blackSqlService;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/proxy/_black_list").methods(HttpMethod.GET, HttpMethod.POST),
                HttpRoute.newRoute().path("/proxy/_black_list/{id}").methods(HttpMethod.DELETE)
        );
    }

    @Override
    public String doHandle(RequestContext context) {
        Map<String, String> urlParams = context.getUrlParams();
        String result;
        if (HttpMethod.GET.name().equals(context.getHttpRequest().getMethod())) {
            int size = urlParams.get("size") == null ? 9999 : Integer.parseInt(urlParams.get("size"));
            result = blackSqlService.getList(size);
        } else if (HttpMethod.DELETE.name().equals(context.getHttpRequest().getMethod())) {
            result = blackSqlService.removeBlackSql(urlParams.get("id"));
        } else {
            String requestBody = context.getRequestInfo().getRequestBody();
            JSONObject requestObj = JSONObject.parseObject(requestBody);
            int range = requestObj.getIntValue("range");
            String sql = requestObj.getString("sql");
            if (range == 0 && StringUtils.isEmpty(sql)) {
                throw new IllegalArgumentException("缺少range或sql参数");
            }
            result = blackSqlService.addBlackSql(range, sql);
        }
        return result;
    }
}
