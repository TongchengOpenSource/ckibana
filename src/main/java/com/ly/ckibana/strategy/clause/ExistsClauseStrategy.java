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
package com.ly.ckibana.strategy.clause;

import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.compute.QueryClause;
import com.ly.ckibana.model.enums.QueryClauseType;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.util.ParamConvertUtils;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.SqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * ExistsClauseStrategy.
 * @author caojiaqiang
 */
@Component
public class ExistsClauseStrategy implements ClauseStrategy {

    @Override
    public QueryClauseType getType() {
        return QueryClauseType.EXISTS;
    }

    @Override
    public String toSql(QueryClause queryClause) {
        CkRequestContext ckRequestContext = queryClause.getCkRequestContext();
        Map<String, String> columns = ckRequestContext.getColumns();
        String queryField = queryClause.getParam().getString(Constants.FIELD);
        String field = ParamConvertUtils.convertUiFieldToCkField(columns, StringUtils.trim(queryField));
        String ckFieldNameSqlPart = ProxyUtils.getFieldSqlPart(field);
        // if this field is string type, we should check value is not null and not empty.
        boolean isStringType = ckRequestContext.getColumns().get(field) != null && ProxyUtils.isString(columns.get(field));
        return SqlUtils.getIsNotNullString(ckFieldNameSqlPart, isStringType);
    }
}
