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

import com.ly.ckibana.configure.web.mapping.MethodHandlers;
import com.ly.ckibana.configure.web.mapping.PathTrie;
import com.ly.ckibana.handlers.BaseHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.web.util.UriUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Register routes.
 * @author caojiaqiang
 */
@Configuration
public class RoutesConfigurer {

    @Bean
    public PathTrie<MethodHandlers> registerRoutes(List<BaseHandler> handlers) {
        PathTrie<MethodHandlers> handlerPathTrie = new PathTrie<>(v -> UriUtils.decode(v, StandardCharsets.UTF_8));
        Map<String, Set<HttpMethod>> seenRoutes = new HashMap<>();

        handlers.forEach(handler -> handler.routes().forEach(handlerRoute -> {
            String handlerRoutePath = handlerRoute.getPath();
            Set<HttpMethod> seenRouteMethods = seenRoutes.computeIfAbsent(handlerRoutePath, k -> new HashSet<>());
            // Add the handler for each method
            Set<HttpMethod> routeMethods = handlerRoute.getMethods();
            // Help with CORS
            routeMethods.add(HttpMethod.OPTIONS);
            routeMethods.forEach(method -> {
                if (seenRouteMethods.contains(method) && method != HttpMethod.OPTIONS) {
                    throw new IllegalStateException("Duplicate route " + method + " " + handlerRoutePath);
                }
                seenRouteMethods.add(method);
                String pathWithPrefix = String.format("/%s", handlerRoutePath);
                pathWithPrefix = pathWithPrefix.replaceAll("/{2,}", "/");
        
                handlerPathTrie.insertOrUpdate(pathWithPrefix, new MethodHandlers(handlerRoutePath, handler, method),
                        (mHandlers, newMHandler) -> mHandlers.addMethods(handler, method));
            });
        }));
        return handlerPathTrie;
    }
}
