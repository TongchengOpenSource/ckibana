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

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.QueryClause;
import com.ly.ckibana.model.enums.BoolCombiningQueryType;
import com.ly.ckibana.model.enums.QueryClauseType;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.util.SqlUtils;
import com.ly.ckibana.util.Utils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.StringJoiner;

@Component
public class BoolClauseStrategy implements ClauseStrategy {

    @Resource
    private ClauseStrategySelector clauseStrategySelector;

    @Override
    public QueryClauseType getType() {
        return QueryClauseType.BOOL;
    }

    @Override
    public String toSql(QueryClause queryClause) {
        CkRequestContext ckRequestContext = queryClause.getCkRequestContext();
        JSONObject param = queryClause.getParam();
        String mustStr = buildBoolQuerySqlByType(BoolCombiningQueryType.MUST, param, ckRequestContext);
        String mustNotStr = buildBoolQuerySqlByType(BoolCombiningQueryType.MUST_NOT, param, ckRequestContext);
        String filterStr = buildBoolQuerySqlByType(BoolCombiningQueryType.FILTER, param, ckRequestContext);
        List<String> tempList = Utils.addNotBlankItemsToList(mustStr, mustNotStr, filterStr);
        // TODO should逻辑
        if (tempList.isEmpty()) {
            String shouldStr = buildBoolQuerySqlByType(BoolCombiningQueryType.SHOULD, param, ckRequestContext);
            tempList = Utils.addNotBlankItemsToList(shouldStr);
        }
        return String.join(SqlUtils.wrapFieldSpace(SqlConstants.AND), tempList);
    }

    /**
     * 将bool查询中的must,must_not,filter,should转为ck sql.
     * @param type 类型
     * @param boolObj boolObj
     * @param ckRequestContext ckRequestContext
     * @return sql
     */
    private String buildBoolQuerySqlByType(BoolCombiningQueryType type, JSONObject boolObj, CkRequestContext ckRequestContext) {
        String result = "";
        JSONArray array = getClauseArray(boolObj, type.name().toLowerCase());
        if (array == null || array.isEmpty()) {
            return result;
        }
        if (BoolCombiningQueryType.MUST.equals(type) || BoolCombiningQueryType.FILTER.equals(type)) {
            result = buildChildSql(" AND ", "(%s)", array, ckRequestContext);
        } else if (BoolCombiningQueryType.MUST_NOT.equals(type)) {
            result = buildChildSql(" AND NOT ", "( NOT %s)", array, ckRequestContext);
        } else if (BoolCombiningQueryType.SHOULD.equals(type)) {
            result = buildChildSql(" OR ", "(%s)", array, ckRequestContext);
        }
        return result;
    }

    /**
     * bool查询的子条件既可能是数组也可能是单个对象，统一转换为数组处理.
     * KQL 的 not 语法会生成 must_not: { ... } 这种单对象结构.
     *
     * @param boolObj bool查询
     * @param key     子条件key
     * @return 子条件数组
     */
    private JSONArray getClauseArray(JSONObject boolObj, String key) {
        Object value = boolObj.get(key);
        if (value instanceof JSONArray) {
            return (JSONArray) value;
        }
        if (value instanceof JSONObject) {
            JSONArray result = new JSONArray();
            result.add(value);
            return result;
        }
        return null;
    }

    private String buildChildSql(String delimiter, String format, JSONArray array, CkRequestContext ckRequestContext) {
        StringJoiner joiner = new StringJoiner(delimiter);
        for (int i = 0; i < array.size(); i++) {
            String tempStr = clauseStrategySelector.buildQuerySql(ckRequestContext, array.getJSONObject(i));
            if (StringUtils.isNotBlank(tempStr)) {
                joiner.add(tempStr);
            }
        }
        if (joiner.length() > 0) {
            return String.format(format, joiner);
        }
        return "";
    }
}
