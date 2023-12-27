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

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.QueryClause;
import com.ly.ckibana.model.enums.QueryClauseType;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ParamConvertUtils;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.SqlUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class TermsClauseStrategy implements ClauseStrategy {

    @Override
    public QueryClauseType getType() {
        return QueryClauseType.TERMS;
    }

    @Override
    public String toSql(QueryClause queryClause) {
        JSONObject itemObject = queryClause.getParam();
        CkRequestContext ckRequestContext = queryClause.getCkRequestContext();
        List<String> itemsByField = new ArrayList<>();
        itemObject.keySet().forEach(each -> {
            List<String> items = new ArrayList<>();
            String ckField = ParamConvertUtils.convertUiFieldToCkField(ckRequestContext.getColumns(), each);
            String ckFieldSqlPart = ProxyUtils.getFieldSqlPart(ckField);
            String ckFieldType = ProxyUtils.getCkFieldTypeByName(ckField, ckRequestContext.getColumns());
            if (ProxyUtils.isString(ckFieldType)) {
                List<String> tempItems = JSONUtils.deserializeToList((List) itemObject.get(ckField), String.class);
                tempItems.forEach(fieldValue -> {
                    items.add(SqlUtils.getEqualsSql(ckFieldSqlPart, fieldValue, true));
                });
            } else {
                List<Object> tempItems = (List) itemObject.get(ckField);
                tempItems.forEach(fieldValue -> {
                    Object value = ProxyUtils.convertValue(ckFieldType, ckField, fieldValue, ckRequestContext.getIndexPattern().getTimeField());
                    items.add(SqlUtils.getEqualsSql(ckFieldSqlPart, value, false));
                });
            }
            String condition = SqlUtils.getConditionString(SqlUtils.wrapFieldSpace(SqlConstants.OR), false, items.toArray(new String[0]));
            itemsByField.add(condition);
        });
        return SqlUtils.getConditionString(SqlUtils.wrapFieldSpace(SqlConstants.AND), false, itemsByField.toArray(new String[0]));
    }
}
