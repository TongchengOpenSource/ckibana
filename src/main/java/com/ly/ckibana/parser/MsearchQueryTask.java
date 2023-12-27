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
package com.ly.ckibana.parser;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.exception.CKNotSupportException;
import com.ly.ckibana.model.exception.UiException;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.util.ProxyUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

@Slf4j
public class MsearchQueryTask implements Callable<Response> {

    private CkRequestContext ckRequestContext;

    private AggResultParser aggsParser;

    private JSONObject searchQuery;

    public MsearchQueryTask(CkRequestContext ckRequestContext, AggResultParser aggResultParser, JSONObject searchQuery) {
        this.ckRequestContext = ckRequestContext;
        this.aggsParser = aggResultParser;
        this.searchQuery = searchQuery;
    }

    @Override
    public Response call() {
        try {
            return invoke(ckRequestContext);
        } catch (CKNotSupportException ex) {
            log.error("call error ", ex);
            throw new CKNotSupportException(ex.getMessage());
        } catch (Exception ex) {
            if (ex instanceof UiException e) {
                return ProxyUtils.newKibanaException(e.getUiShow());
            }
            log.error("call error, msearchQuery:{}, ", searchQuery, ex);
            return ProxyUtils.newKibanaException(ex.getMessage());
        }
    }

    /**
     * 查询.
     *
     * @param ckRequestContext 请求参数
     * @return ResponseItem
     * @throws Exception 异常
     */
    public Response invoke(CkRequestContext ckRequestContext) throws Exception {
        return aggsParser.buildMSearchAggResult(ckRequestContext);
    }
}
