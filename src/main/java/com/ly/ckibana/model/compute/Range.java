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

import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.util.ProxyUtils;
import lombok.Data;

@Data
public class Range {

    private String ckFieldName;

    private String ckFieldType;

    private boolean lessThanEq;

    private Object low;

    private boolean moreThanEq;

    private Object high;

    public Range() {
    }

    public Range(String ckFieldName, String ckFieldType) {
        this.ckFieldName = ckFieldName;
        this.ckFieldType = ckFieldType;
    }

    public Range(String ckFieldName, String ckFieldType, Object high, Object low) {
        this.ckFieldName = ckFieldName;
        this.ckFieldType = ckFieldType;
        this.high = high;
        this.low = low;
    }

    /**
     * range请求包装。将不支持range的原始字段转换为支持range的ck function包装后的值,并利用比较运算符组装为最终sql.
     * 1)时间字段：支持数值型timestamp 和DateTime, DateTime64存储类型
     * 2)ip字段：支持字符串存储类型
     * 3)普通数值字段：支持数值型存储类型
     * 具体clickhouse函数参见方法getRangeWrappedBySqlFunction()
     *
     * @param isTimeField 是否为indexPattern的时间字段
     * @return sql
     */
    public String toSql(boolean isTimeField) {
        String result = "";
        Range rangeWrappedBySqlFunction = ProxyUtils.getRangeWrappedBySqlFunction(this, isTimeField);
        Object ckFieldSqlValue = rangeWrappedBySqlFunction.getCkFieldName();

        if (high != null && low != null) {
            result = String.format(" %s AND %s ", getRangeHighSql(ckFieldSqlValue, rangeWrappedBySqlFunction.getHigh()),
                    getRangeLowSql(ckFieldSqlValue, rangeWrappedBySqlFunction.getLow()));
        } else if (high == null && low != null) {
            result = getRangeLowSql(ckFieldSqlValue, rangeWrappedBySqlFunction.getLow());
        } else if (high != null && low == null) {
            result = getRangeHighSql(ckFieldSqlValue, rangeWrappedBySqlFunction.getHigh());
        }
        result = String.format("(%s)", result);
        return result;
    }

    private String getRangeLowSql(Object ckFieldSqlValue, Object lowSqlValue) {
        return String.format(" %s %s %s ", ckFieldSqlValue, moreThanEq ? Constants.Symbol.GTE : Constants.Symbol.GT, lowSqlValue);
    }

    private String getRangeHighSql(Object ckFieldSqlValue, Object highSqlValue) {
        return String.format(" %s %s %s ", ckFieldSqlValue, lessThanEq ? Constants.Symbol.LTE : Constants.Symbol.LT, highSqlValue);
    }
    
    public Long getDiffMillSeconds() {
        return (Long) this.getHigh() - (Long) this.getLow();
    }
}
