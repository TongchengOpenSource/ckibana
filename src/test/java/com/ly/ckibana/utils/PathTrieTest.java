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
package com.ly.ckibana.utils;

import com.ly.ckibana.configure.web.mapping.MethodHandlers;
import com.ly.ckibana.configure.web.mapping.PathTrie;
import com.ly.ckibana.handlers.BaseHandler;
import com.ly.ckibana.handlers.DefaultHandler;
import com.ly.ckibana.handlers.SearchHandler;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpMethod;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class PathTrieTest {

    PathTrie<MethodHandlers> handlerPathTrie = new PathTrie<>();
    SearchHandler searchHandler = new SearchHandler();
    DefaultHandler defaultHandler = new DefaultHandler();

    @Before
    public void initPathTrie() {
        String searchRoutePath = "/{index}/_search";
        handlerPathTrie.insertOrUpdate(searchRoutePath, new MethodHandlers(searchRoutePath, searchHandler, HttpMethod.GET, HttpMethod.PUT),
                (mHandlers, newMHandler) -> mHandlers.addMethods(searchHandler, HttpMethod.GET, HttpMethod.PUT));

        String fallbackRoutePath0 = "/*";
        handlerPathTrie.insertOrUpdate(fallbackRoutePath0, new MethodHandlers(fallbackRoutePath0, defaultHandler, HttpMethod.values()),
                (mHandlers, newMHandler) -> mHandlers.addMethods(defaultHandler, HttpMethod.values()));

        String fallbackRoutePath2 = "/*/*";
        handlerPathTrie.insertOrUpdate(fallbackRoutePath2, new MethodHandlers(fallbackRoutePath2, defaultHandler, HttpMethod.values()),
                (mHandlers, newMHandler) -> mHandlers.addMethods(defaultHandler, HttpMethod.values()));
    }

    @Test
    public void tests() {
        List.of(
                Triple.of("/a/b/_search", HttpMethod.GET, searchHandler),
                Triple.of("/a/b/_search", HttpMethod.PUT, searchHandler),
                Triple.of("/a/b/_search", HttpMethod.POST, null),

                Triple.of("/", HttpMethod.POST, defaultHandler),
                Triple.of("/a", HttpMethod.DELETE, defaultHandler),
                Triple.of("/a/b", HttpMethod.DELETE, defaultHandler),
                Triple.of("/a/b/c", HttpMethod.DELETE, defaultHandler),
                Triple.of("/a/b/c/d", HttpMethod.DELETE, null)
        ).forEach(triple -> {
            Iterator<MethodHandlers> methodHandlersIterator = handlerPathTrie.retrieveAll(triple.getLeft(), HashMap::new);
            while (methodHandlersIterator.hasNext()) {
                MethodHandlers next = methodHandlersIterator.next();
                if (next == null) {
                    continue;
                }
                BaseHandler baseHandler = next.getBaseHandler(triple.getMiddle());
                assert baseHandler == triple.getRight();
            }
        });
    }
}
