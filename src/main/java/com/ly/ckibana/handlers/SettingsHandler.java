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
import com.ly.ckibana.model.property.KibanaProperty;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class SettingsHandler extends BaseHandler {

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/config/all").methods(HttpMethod.POST, HttpMethod.GET)
        );
    }

    @Override
    public String doHandle(RequestContext context) {
        if (HttpMethod.GET.name().equals(context.getHttpRequest().getMethod())) {
            return JSONUtils.serialize(proxyConfigLoader.getKibanaProperty());
        }
        KibanaProperty newKibanaProperty = JSON.parseObject(context.getRequestInfo().getRequestBody(), KibanaProperty.class);
        if (newKibanaProperty == null) {
            return ProxyUtils.getErrorResponse("请求体不能为空");
        }

        try {
            String body = proxyConfigLoader.getYaml().dumpAs(newKibanaProperty, Tag.MAP, DumperOptions.FlowStyle.BLOCK);
            log.info("update settings:[{}], body:{}", proxyConfigLoader.getSettingsIndexName(), body);
            String response = EsClientUtil.saveOne(proxyConfigLoader.getMetadataRestClient(), proxyConfigLoader.getSettingsIndexName(),
                    "kibana",
                    Map.of(
                            "key", "kibana",
                            "value", body
                    ),
                    proxyConfigLoader.getMajorVersion());
            log.info("update settings response:[{}]", response);
            return response;
        } catch (Exception e) {
            return ProxyUtils.getErrorResponse(e);
        }
    }
}
