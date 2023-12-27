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
import com.google.common.base.Strings;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.QueryClause;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.enums.QueryClauseType;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.util.DateUtils;
import com.ly.ckibana.util.ParamConvertUtils;
import com.ly.ckibana.util.ProxyUtils;
import org.springframework.stereotype.Component;

@Component
public class RangeClauseStrategy implements ClauseStrategy {

    @Override
    public QueryClauseType getType() {
        return QueryClauseType.RANGE;
    }

    @Override
    public String toSql(QueryClause queryClause) {
        CkRequestContext ckRequestContext = queryClause.getCkRequestContext();
        String result = "";
        String field = queryClause.getParam().keySet().iterator().next();
        JSONObject rangeParam = queryClause.getParam().getJSONObject(field);
        String rangFieldName = ParamConvertUtils.convertUiFieldToCkField(ckRequestContext.getColumns(), field);
        Range range = new Range(ProxyUtils.getFieldSqlPart(rangFieldName),
                ProxyUtils.getCkFieldTypeByName(rangFieldName, ckRequestContext.getColumns()));
        boolean isArrayType = ProxyUtils.isArrayType(range.getCkFieldType());
        IndexPattern indexPattern = ckRequestContext.getIndexPattern();
        if (Strings.isNullOrEmpty(rangFieldName) || isArrayType) {
            return result;
        }
        //解析 range值，format上面已经解析
        for (String each : rangeParam.keySet()) {
            if (Constants.FORMAT.equals(each)) {
                continue;
            }
            Object value = rangeParam.get(each);
            if (isTimeFieldValueString(rangFieldName, indexPattern, value)) {
                value = DateUtils.toEpochMilli(value);
            }
            if (SqlConstants.LT_NAME.equals(each) || SqlConstants.LTE_NAME.equals(each)) {
                range.setLessThanEq(SqlConstants.LTE_NAME.equals(each));
                range.setHigh(value);
            } else if (SqlConstants.GT_NAME.equals(each) || SqlConstants.GTE_NAME.equals(each)) {
                range.setMoreThanEq(SqlConstants.GTE_NAME.equals(each));
                range.setLow(value);
            }
        }
        if (rangFieldName.equals(indexPattern.getTimeField())) {
            ckRequestContext.setTimeRange(range);
        }
        // 若是时间字段range，不使用toSql转换，因为后续会单独处理（时间round+hits查询优化策略）
        if (!indexPattern.getTimeField().equals(rangFieldName)) {
            result = range.toSql(false);
        }
        return result;
    }

    /**
     * 是否为时间字段range,且参数格式化为字符类型。支持utc时间和gmt两种时间格式解析.
     * @param rangFieldName 时间字段
     * @param indexPattern 索引模式
     * @param value 参数值
     * @return 是否为时间字段range
     */
    private boolean isTimeFieldValueString(String rangFieldName, IndexPattern indexPattern, Object value) {
        return indexPattern.getTimeField().equals(rangFieldName) && null != value && value instanceof String;
    }
}
