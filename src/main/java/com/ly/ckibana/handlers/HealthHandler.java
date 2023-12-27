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

import com.ly.ckibana.configure.web.route.HttpRoute;
import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class HealthHandler extends BaseHandler {
    @Override
    public List<HttpRoute> routes() {
        return List.of(HttpRoute.newRoute().path("/health").methods(HttpMethod.values()));
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
        IOUtils.write("success", response.getOutputStream(), StandardCharsets.UTF_8);
    }
}
