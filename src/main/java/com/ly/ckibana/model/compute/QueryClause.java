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
package com.ly.ckibana.model.compute;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.enums.QueryClauseType;
import com.ly.ckibana.model.request.CkRequestContext;
import lombok.Data;

@Data
public class QueryClause {

    private String ckField;

    private String ckFieldType;

    private CkRequestContext ckRequestContext;

    private JSONObject param;

    private QueryClauseType type;

    public QueryClause(CkRequestContext ckRequestContext, JSONObject param, QueryClauseType type) {
        this.ckRequestContext = ckRequestContext;
        this.param = param;
        this.type = type;
    }
}
