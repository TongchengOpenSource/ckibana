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
package com.ly.ckibana.configure.web.route;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpMethod;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@AllArgsConstructor
@Getter
public class HttpRoute {
    private String path;
    
    private Set<HttpMethod> methods;

    public static HttpRoute newRoute() {
        return new HttpRoute("", new HashSet<>());
    }

    public HttpRoute path(String path) {
        this.path = path;
        return this;
    }

    public HttpRoute methods(HttpMethod... methods) {
        this.methods.addAll(List.of(methods));
        return this;
    }
}
