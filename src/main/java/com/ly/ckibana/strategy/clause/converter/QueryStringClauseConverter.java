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
package com.ly.ckibana.strategy.clause.converter;

import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.QueryStringField;
import com.ly.ckibana.model.enums.IPType;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.QueryConvertUtils;
import com.ly.ckibana.util.SqlUtils;
import com.ly.ckibana.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.StringJoiner;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryStringClauseConverter {

    public static final String LOGICALOP_RANGE_TEMPLATE_DEFAULT = "%s ( %s AND %s)";

    private String logicalOp;

    private QueryStringField field;

    private String expr;

    private String content;

    /**
     * 默认为忽略大小检索.
     *
     */
    public String toSql() {
        if (StringUtils.isNotBlank(content)) {
            return content;
        }
        String value = Utils.trimByPrefixAndSuffixChars(getExpr(), Constants.Symbol.DOUBLE_QUOTA, Constants.Symbol.DOUBLE_QUOTA);
        String ckFieldNameSql = ProxyUtils.getFieldSqlPart(field.getCkName());
        field.setCkName(ckFieldNameSql);
        // range操作
        if (isRangeQuery()) {
            return generateRangeSql();
        } else if (isInQuery()) {
            return generateInSql(value);
        }
        return generateSql(value, ckFieldNameSql);
    }

    /**
     * 基于ck字段类型得到运算解析sql.
     * 适用于:和ui运算逻辑一致，无需二次变更,支持等于、不等于、相似,不相似，大于(等于)，小于(等于)等检索逻辑
     *
     * @param value           值
     * @param ckFieldNameSql ck字段名
     * @return sql
     */
    private String generateSql(String value, String ckFieldNameSql) {
        IPType ipType = ProxyUtils.getIpType(field.getCkType(), value);
        if (ProxyUtils.isNumeric(field.getCkType())) {
            return generateNumericSql(value);
        } else if (ProxyUtils.isString(field.getCkType())) {
            return String.format("%s %s", logicalOp, QueryConvertUtils.convertMatchPhraseToSql(field.getName(), field.getCkName(), value, field.getCkType()));
        } else if (null != ipType && ipType.getIsCkIpType()) {
            String ipSqlCkFieldName = SqlUtils.generateIpSql(ckFieldNameSql, ipType, Boolean.FALSE);
            String ipSqlValue = SqlUtils.generateIpSql(value, ipType, Boolean.TRUE);
            return String.format("%s %s", logicalOp, SqlUtils.getEqualsSql(ipSqlCkFieldName, ipSqlValue, Boolean.FALSE));
        } else {
            return String.format("%s %s", logicalOp, SqlUtils.getEqualsSql(ckFieldNameSql, value, Boolean.TRUE));
        }
    }

    private boolean isRangeQuery() {
        return expr.contains(SqlUtils.wrapFieldSpace(SqlConstants.TO));
    }

    /**
     * 生成range查询参数.
     * expr:["3.3.3.3" TO "4.4.4.4"]
     *
     * @return sql
     */
    private String generateRangeSql() {
        boolean includeThreshold = expr.startsWith(Constants.Symbol.LEFT_BRACKET);
        String from = getFromValue();
        String to = getToValue();
        IPType ipType = ProxyUtils.getIpType(field.getCkType(), from);
        if (null != ipType) {
            field.setCkName(SqlUtils.generateIpSql(field.getCkName(), ipType, false));
            from = SqlUtils.generateIpSql(from, ipType, true);
            to = SqlUtils.generateIpSql(to, ipType, true);
        }
        return String.format(LOGICALOP_RANGE_TEMPLATE_DEFAULT,
                logicalOp,
                SqlUtils.getCompareSql(field.getCkName(), includeThreshold ? Constants.Symbol.GTE : Constants.Symbol.GT, from),
                SqlUtils.getCompareSql(field.getCkName(), includeThreshold ? Constants.Symbol.LTE : Constants.Symbol.LT, to));
    }
    
    /**
     * 基于range参数格式获取from值.
     * expr:["3.3.3.3" TO "4.4.4.4"]
     *
     * @return 3.3.3.3
     */
    private String getFromValue() {
        return ProxyUtils.getPurValue(expr.split(SqlUtils.wrapFieldSpace(SqlConstants.TO))[0].substring(1));
    }

    /**
     * 基于range参数格式获取to值.
     * expr:["3.3.3.3" TO "4.4.4.4"]
     *
     * @return 4.4.4.4
     */
    private String getToValue() {
        return ProxyUtils.getPurValue(expr.split(SqlUtils.wrapFieldSpace(SqlConstants.TO))[1].substring(0, expr.split(SqlUtils.wrapFieldSpace(SqlConstants.TO))[1].length() - 1));
    }

    private boolean isInQuery() {
        return ProxyUtils.isRangeQuery(expr);
    }

    /**
     * 生成sql 字段名 运算符 值.
     */
    private String generateInSql(String value) {
        String pureValues = ProxyUtils.getPurValue(value);
        if (ProxyUtils.isString(getField().getCkType())) {
            //字符类型 将in查询转换为or 查询。支持精确,模糊，忽略大小写检索功能
            return generateStringInSql(pureValues);
        } else {
            return generateNotStringInSql(value, pureValues);
        }

    }

    /**
     * 将非string类型in,key,value转换后执行in操作.
     *
     * @param value      value
     * @param pureValues pureValues
     * @return sql
     */
    private String generateNotStringInSql(String value, String pureValues) {
        IPType ipType = ProxyUtils.getIpType(field.getCkType(), value);
        String sqlField = (ipType != null && ipType.getIsCkIpType())
                ? SqlUtils.generateIpSql(field.getCkName(), ipType, Boolean.FALSE) : field.getCkName();
        StringJoiner valueJoiner = new StringJoiner(Constants.Symbol.COMMA_QUOTA);
        for (String each : pureValues.split(Constants.Symbol.COMMA_QUOTA)) {
            String sqlValue = ProxyUtils.getPurValue(each);
            if (null != ipType && ipType.getIsCkIpType()) {
                sqlValue = SqlUtils.generateIpSql(each, ipType, Boolean.TRUE);
            }
            valueJoiner.add(sqlValue);
        }
        return String.format("%s %s", logicalOp, SqlUtils.getInString(sqlField, valueJoiner.toString()));
    }

    /**
     * 将string in 查询，转为or逻辑实现.
     */
    private String generateStringInSql(String pureValues) {
        StringJoiner joiner = new StringJoiner(SqlUtils.wrapFieldSpace(SqlConstants.OR));
        for (String each : pureValues.split(Constants.Symbol.COMMA_QUOTA)) {
            String sqlName = field.getCkName();
            String sqlValue = ProxyUtils.getPurValue(each);
            //.caseIngore或.caseIgnore.keyword
            if (ProxyUtils.isCaseIgnoreKeywordStringQuery(field.getName())) {
                sqlName = SqlUtils.getLowerString(sqlName);
                sqlValue = sqlValue.toLowerCase();
            }
            String partSql = ProxyUtils.isPreciseField(field.getName())
                    ? SqlUtils.getEqualsSql(sqlName, sqlValue, Boolean.TRUE)
                    : (ProxyUtils.isCaseIgnoreQuery(field.getName())
                        ? SqlUtils.getCaseIgnoreString(sqlName, sqlValue)
                        : SqlUtils.getLikeByType(sqlName, sqlValue, field.getCkType()));
            joiner.add(partSql);
        }
        return String.format("%s %s", logicalOp, SqlUtils.wrapFiledNameWithParenthesis(joiner.toString()));
    }

    private String generateNumericSql(String value) {
        String format;
        //数值类 存在比较运算符
        if (value.startsWith(Constants.Symbol.LT) || value.startsWith(Constants.Symbol.GT)) {
            format = "%s %s  %s";
        } else {
            format = "%s %s = %s";
        }
        return formatToSql(format, value);
    }

    private String formatToSql(String format, String value) {
        return String.format(format, logicalOp, field.getCkName(), value);
    }
}
