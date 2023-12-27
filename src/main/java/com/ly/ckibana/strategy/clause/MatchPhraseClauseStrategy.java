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
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.compute.QueryClause;
import com.ly.ckibana.model.enums.IPType;
import com.ly.ckibana.model.enums.QueryClauseType;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.util.ParamConvertUtils;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.QueryConvertUtils;
import com.ly.ckibana.util.SqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class MatchPhraseClauseStrategy implements ClauseStrategy {

    @Override
    public QueryClauseType getType() {
        return QueryClauseType.MATCH_PHRASE;
    }

    @Override
    public String toSql(QueryClause queryClause) {
        CkRequestContext ckRequestContext = queryClause.getCkRequestContext();
        String result = "";
        Map<String, Object> mapParam = new HashMap<>(queryClause.getParam());
        String field = mapParam.keySet().iterator().next();
        String ckFieldName = ParamConvertUtils.convertUiFieldToCkField(ckRequestContext.getColumns(), field);
        String ckFieldNameSqlPart = ProxyUtils.getFieldSqlPart(ckFieldName);
        String type = ProxyUtils.getCkFieldTypeByName(ckFieldName, ckRequestContext.getColumns());
        Object body = parseBody(mapParam.get(field), ckFieldName, type, ckRequestContext.getIndexPattern().getTimeField());
        IPType ipType = ProxyUtils.getIpType(type, body.toString());
        if (ProxyUtils.isString(type)) {
            result = parseByStringType(field, ckFieldNameSqlPart, type, body);
        } else if (ProxyUtils.isNumeric(type)) {
            result = parseByNumericType(ckFieldNameSqlPart, type, body);
        } else if (null != ipType) {
            ckFieldNameSqlPart = SqlUtils.generateIpSql(ckFieldNameSqlPart, ipType, Boolean.FALSE);
            body = SqlUtils.generateIpSql(body.toString(), ipType, Boolean.TRUE);
            result = SqlUtils.getEqualsSql(ckFieldNameSqlPart, body, Boolean.FALSE);
        } else {
            result = SqlUtils.getEqualsSql(ckFieldNameSqlPart, body, Boolean.TRUE);
        }
        return result;
    }

    private String parseByNumericType(String ckFieldNameSqlPart, String type, Object body) {
        String result;
        if (ProxyUtils.isArrayType(type)) {
            result = SqlUtils.getHasString(ckFieldNameSqlPart, body, false);
        } else {
            result = SqlUtils.getEqualsSql(ckFieldNameSqlPart, body, false);
        }
        return result;
    }

    private String parseByStringType(String field, String ckFieldNameSqlPart, String type, Object body) {
        String result;
        if (ProxyUtils.isArrayType(type)) {
            result = SqlUtils.getHasString(ckFieldNameSqlPart, body, true);
        } else {
            String strPart = QueryConvertUtils.convertMatchPhraseToSql(field, ckFieldNameSqlPart, body, type);
            result = StringUtils.isNotBlank(strPart) ? String.format("(%s)", strPart) : null;
        }
        return result;
    }

    /**
     * 解析实际value中.
     * 如match_phrase中  is对应格式为 field:{query:value}
     */
    private Object parseBody(Object queryBody, String ckFieldName, String type, String timeField) {
        Object value = queryBody instanceof JSONObject ? ((JSONObject) queryBody).get(Constants.QUERY) : queryBody;
        value = ProxyUtils.convertValue(type, ckFieldName, value, timeField);
        return value;
    }
}
