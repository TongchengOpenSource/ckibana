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
package com.ly.ckibana.configure.web.mapping;

import com.ly.ckibana.handlers.BaseHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.handler.AbstractHandlerMapping;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Configuration
public class PathTrieHandlerMapping extends AbstractHandlerMapping {
    public static final String URL_PARAMS = "PathTrieHandlerMapping.urlParams";
    
    private final PathTrie<MethodHandlers> handlerPathTrie;

    private final BaseHandler notFoundHandler;

    public PathTrieHandlerMapping(PathTrie<MethodHandlers> handlerPathTrie, BaseHandler notFoundHandler, List<HandlerInterceptor> handlerInterceptors) {
        this.handlerPathTrie = handlerPathTrie;
        this.notFoundHandler = notFoundHandler;
        this.setInterceptors(handlerInterceptors.toArray());
    }

    @Override
    public int getOrder() {
        return Integer.MIN_VALUE;
    }

    @Override
    protected Object getHandlerInternal(HttpServletRequest request) {
        Map<String, String> urlParams = new HashMap<>();
        Iterator<MethodHandlers> handlers = getAllHandlers(urlParams, request.getRequestURI());
        while (handlers.hasNext()) {
            MethodHandlers methodHandlers = handlers.next();
            if (methodHandlers == null) {
                continue;
            }
            BaseHandler baseHandler = methodHandlers.getBaseHandler(HttpMethod.valueOf(request.getMethod()));
            if (baseHandler == null) {
                continue;
            }
            if (!urlParams.isEmpty()) {
                request.setAttribute(URL_PARAMS, urlParams);
            }
            return baseHandler;
        }
        return notFoundHandler;
    }

    Iterator<MethodHandlers> getAllHandlers(Map<String, String> requestParamsRef, String rawPath) {
        final Supplier<Map<String, String>> paramsSupplier;
        if (requestParamsRef == null) {
            paramsSupplier = () -> null;
        } else {
            // Between retrieving the correct path, we need to reset the parameters,
            // otherwise parameters are parsed out of the URI that aren't actually handled.
            final Map<String, String> originalParams = new HashMap<>(requestParamsRef);
            paramsSupplier = () -> {
                // PathTrie modifies the request, so reset the params between each iteration
                requestParamsRef.clear();
                requestParamsRef.putAll(originalParams);
                return requestParamsRef;
            };
        }
        // we use rawPath since we don't want to decode it while processing the path resolution
        // so we can handle things like:
        // my_index/my_type/http%3A%2F%2Fwww.google.com
        return handlerPathTrie.retrieveAll(rawPath, paramsSupplier);
    }
}
