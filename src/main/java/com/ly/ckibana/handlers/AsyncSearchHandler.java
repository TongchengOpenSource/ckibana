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
import com.ly.ckibana.model.exception.FallbackToEsException;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.parser.ParamParser;
import com.ly.ckibana.parser.SearchParser;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Component
public class AsyncSearchHandler extends BaseHandler {

    @Resource
    private ParamParser paramParser;


    @Resource
    private SearchParser searchParser;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/_async_search").methods(HttpMethod.GET, HttpMethod.POST),
                HttpRoute.newRoute().path("/{index}/_async_search").methods(HttpMethod.GET, HttpMethod.POST)
        );
    }

    @Override
    public String doHandle(RequestContext context) throws Exception {
        String index = context.getIndex();
        if (!context.isCkIndex()) {
            throw new FallbackToEsException();
        }
        return searchParser.execute(context, index, context.getProxyConfig().buildIndexPattern(index), true);
    }
}
