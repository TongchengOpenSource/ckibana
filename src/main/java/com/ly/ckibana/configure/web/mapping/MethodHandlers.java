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
import org.springframework.http.HttpMethod;
import org.springframework.lang.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulate multiple BaseHandlers for the same path, allowing different BaseHandlers for different HTTP verbs.
 */
public final class MethodHandlers {

    private final String path;
    
    private final Map<HttpMethod, BaseHandler> methodHandlers;

    public MethodHandlers(String path, BaseHandler baseHandler, HttpMethod... methods) {
        this.path = path;
        this.methodHandlers = new HashMap<>(methods.length);
        for (HttpMethod method : methods) {
            methodHandlers.put(method, baseHandler);
        }
    }

    /**
     * Add a BaseHandler for an additional array of methods. Note that {@code MethodBaseHandlers}
     * does not allow replacing the BaseHandler for an already existing method.
     */
    public MethodHandlers addMethods(BaseHandler baseHandler, HttpMethod... methods) {
        for (HttpMethod method : methods) {
            BaseHandler existing = methodHandlers.putIfAbsent(method, baseHandler);
            if (existing != null) {
                throw new IllegalArgumentException("Cannot replace existing BaseHandler for [" + path + "] for method: " + method);
            }
        }
        return this;
    }

    /**
     * Returns the BaseHandler for the given method or {@code null} if none exists.
     */
    @Nullable
    public BaseHandler getBaseHandler(HttpMethod method) {
        return methodHandlers.get(method);
    }

    /**
     * Return a set of all valid HTTP methods for the particular path.
     */
    public Set<HttpMethod> getValidMethods() {
        return methodHandlers.keySet();
    }

    public Collection<BaseHandler> getAllBaseHandlers() {
        return methodHandlers.values();
    }
}
