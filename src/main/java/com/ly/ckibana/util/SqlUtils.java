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
package com.ly.ckibana.util;

import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.enums.IPType;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class SqlUtils {

    /**
     * 排序.
     *
     * @param fieldName 字段名
     * @param isDesc    true: 倒排, false: 正排
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 11:21
     */
    public static String getOrderString(String fieldName, boolean isDesc) {
        if (isDesc) {
            return String.format("%s desc", fieldName);
        }
        return String.format("%s asc", fieldName);
    }

    /**
     * 获取排序字符串.
     * get order string
     *
     * @param fieldName 字段名
     * @param order     排序方式字符串，asc/desc
     * @return java.lang.String
     */
    public static String getOrderString(String fieldName, String order) {
        return String.format("%s %s", fieldName, order);
    }

    /**
     * wrap field name with ().
     *
     * @param fieldName 字段名
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 14:51
     */
    public static String wrapFiledNameWithParenthesis(String fieldName) {
        return Constants.Symbol.LEFT_PARENTHESIS + fieldName + Constants.Symbol.RIGHT_PARENTHESIS;
    }

    /**
     * sql不支持字符，转义处理.
     *
     * @param value 值
     * @return java.lang.String
     */
    public static String escape(String value) {
        return String.format("`%s`", value);
    }

    /**
     * wrap field name with.
     *
     * @param fieldName 字段名
     * @return java.lang.String
     */
    public static String wrapFiledNameSingleQuota(String fieldName) {
        return Constants.Symbol.SINGLE_QUOTA + fieldName + Constants.Symbol.SINGLE_QUOTA;
    }

    /**
     * get not missing sql string.
     *
     * @param fieldName 字段名
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 14:55
     */
    public static String getIsNotMissingSql(String fieldName, Object defaultValue) {
        return String.format("`%s` is not null AND `%s` != %s ", fieldName, fieldName, defaultValue);
    }

    /**
     * get compare two field sql.
     * [A >= B]
     *
     * @param leftValue  左值
     * @param symbol     符号
     * @param rightValue 右值
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 17:30
     */
    public static String getCompareSql(String leftValue, String symbol, Object rightValue) {
        return leftValue + wrapFieldSpace(symbol) + rightValue;
    }

    /**
     * wrap field with space.
     * [ A ]
     *
     * @param fieldName 字段名
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 17:35
     */
    public static String wrapFieldSpace(String fieldName) {
        return Constants.Symbol.SINGLE_SPACE_STRING + fieldName + Constants.Symbol.SINGLE_SPACE_STRING;
    }

    /**
     * get not equal string.
     * [A != B]
     *
     * @param fieldName 字段名
     * @param value     值
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 19:13
     */
    public static String getNotEqualString(String fieldName, Object value) {
        return String.format("%s != %s", fieldName, value);
    }

    /**
     * get function string.
     * [A(B)]
     *
     * @param function 函数名
     * @param value    值
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 19:14
     */
    public static String getFunctionString(String function, Object value) {
        return String.format("%s(%s)", function, value);
    }

    /**
     * get function string.
     * [A(B)]
     *
     * @param function   函数名
     * @param expression 表达式(值)
     * @param values     值列表
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 19:41
     */
    public static String getFunctionString(String function, String expression, Object... values) {
        return getFunctionString(function, String.format(expression, values));
    }

    /**
     * get alias sql string.
     * [A as 'B']
     *
     * @param fieldName 字段名
     * @param alias     别名
     * @return java.lang.String
     */
    public static String getColumnAsAliasString(String fieldName, String alias) {
        return String.format("%s as `%s`", fieldName, alias);
    }

    /**
     * get count if sql string.
     * [countIf(A)B as C]
     *
     * @param condition 条件
     * @param operator  操作
     * @param alias     别名
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 19:59
     */
    public static String getCountIfString(Object condition, Object operator, String alias) {
        return getColumnAsAliasString(String.format("%s%s", getFunctionString(SqlConstants.COUNT_IF, condition), operator), alias);
    }

    /**
     * get count sql string.
     * [count(A)B as C]
     *
     * @param condition 条件
     * @param operator  操作
     * @param alias     别名
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 19:59
     */
    public static String getCountString(Object condition, Object operator, String alias) {
        return getColumnAsAliasString(String.format("%s%s", getFunctionString(SqlConstants.COUNT, condition), operator), alias);
    }

    /**
     * get like string.
     * 默认前后匹配[A like '%B%']
     * 若存在%查询值，则以用户匹配规则生效
     *
     * @param fieldName 字段名
     * @param value     值
     * @param type      字段类型
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 20:46
     */
    public static String getLikeByType(String fieldName, String value, String type) {
        IPType ipType = ProxyUtils.getIpType(type, value);
        if (null != ipType && ipType.getIsCkIpType()) {
            return String.format("%s %s", generateIpSql(fieldName, ipType, Boolean.FALSE), generateIpSql(value, ipType, Boolean.TRUE));
        } else if (value.contains(Constants.CK_LIKE_SPLIT)) {
            return String.format("%s like '%s'", fieldName, value);
        }
        return String.format("%s like '%%%s%%'", fieldName, value);
    }

    /**
     * get in string.
     * [A in (B)
     *
     * @param fieldName 字段名
     * @param inString  in字符串
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 21:03
     */
    public static String getInString(String fieldName, String inString) {
        return String.format("%s in (%s)", fieldName, inString);
    }

    /**
     * get condition string.
     * [(A AND B AND C)] or [((A) AND (B) AND (C))]
     *
     * @param operator           操作符
     * @param splitByParenthesis 按括号拆分
     * @param conditions         条件
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/27 11:15
     */
    public static String getConditionString(String operator, boolean splitByParenthesis, String... conditions) {
        StringBuilder stringBuilder = new StringBuilder();
        if (conditions == null || conditions.length == 0) {
            return StringUtils.EMPTY;
        }
        stringBuilder.append(Constants.Symbol.LEFT_PARENTHESIS);
        for (String each : Arrays.stream(conditions).toArray(String[]::new)) {
            stringBuilder.append(Constants.Symbol.SINGLE_SPACE_STRING);
            if (splitByParenthesis) {
                stringBuilder.append(Constants.Symbol.LEFT_PARENTHESIS);
            }
            stringBuilder.append(each);
            if (splitByParenthesis) {
                stringBuilder.append(Constants.Symbol.RIGHT_PARENTHESIS);
            }
            stringBuilder.append(Constants.Symbol.SINGLE_SPACE_STRING);
            stringBuilder.append(operator);
        }
        stringBuilder.deleteCharAt(1);
        stringBuilder.delete(stringBuilder.length() - operator.length() - 1, stringBuilder.length());
        stringBuilder.append(Constants.Symbol.RIGHT_PARENTHESIS);
        return stringBuilder.toString();
    }

    /**
     * get equals string.
     * [(A = 'B') or (A = B)]
     *
     * @param left                   左值
     * @param right                  右值
     * @param isValueNeedSingleQuota 是否字符串
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/27 11:56
     */
    public static String getEqualsSql(String left, Object right, boolean isValueNeedSingleQuota) {
        if (isValueNeedSingleQuota) {
            return String.format("(%s = '%s')", left, right);
        }
        return String.format("(%s = %s)", left, right);
    }

    /**
     * Determine if a logical operator is an SQL operator.
     * "and", "or", "xor", "not", "and not"
     *
     * @param operator 操作符
     * @return boolean
     * @author quzhihao
     * @date 2023/9/27 14:53
     */
    public static boolean isLogicalOperator(String operator) {
        String lowerCaseOperator = operator.toLowerCase();
        return SqlConstants.LOGICAL_OPERATOR.contains(lowerCaseOperator);
    }

    /**
     * 是否为括号.
     *
     * @param value 值
     * @return boolean
     */
    public static boolean isParenthesisSymbol(String value) {
        return Constants.Symbol.LEFT_PARENTHESIS.equals(value) || Constants.Symbol.RIGHT_PARENTHESIS.equals(value);
    }

    /**
     * 是否为冒号.
     *
     * @param value 值
     * @return boolean
     */
    public static boolean isColonSymbol(String value) {
        return Constants.Symbol.COLON.equals(value);
    }

    /**
     * 获取IsNotNull.
     *
     * @param field    字段
     * @param isString 是否为字符串
     * @return String
     */
    public static String getIsNotNullString(String field, boolean isString) {
        if (isString) {
            return String.format("(isNotNull(%s) AND %s != '')", field, field);
        }
        return String.format("(isNotNull(%s))", field);
    }

    /**
     * has.
     *
     * @param field    字段
     * @param str      字符串
     * @param isString 是否为字符串
     * @return String
     */
    public static String getHasString(String field, Object str, boolean isString) {
        if (isString) {
            return String.format("(has(%s,'%s'))", field, str);
        }
        return String.format("(has(%s,%s))", field, str);
    }

    /**
     * get string caseInsensitive.
     * 忽略大小写检索
     *
     * @param fieldName  字段名
     * @param queryValue 查询值
     * @return String
     */
    public static String getCaseIgnoreString(String fieldName, String queryValue) {
        return String.format(SqlConstants.FUNCTION_FOR_CASE_INSENSITIVE, fieldName, queryValue);
    }

    /**
     * get lower(String).
     * 转小写
     *
     * @param value value
     * @return String
     */
    public static String getLowerString(String value) {
        return String.format(SqlConstants.FUNCTION_TO_LOWER, value);
    }

    /**
     * ip类查询sql转换.
     *
     * @param value        value
     * @param ipType       ipType
     * @param isQueryValue isQueryValue
     * @return String
     */
    public static String generateIpSql(Object value, IPType ipType, boolean isQueryValue) {
        if (isQueryValue) {
            return StringUtils.isNotBlank(ipType.getCkValueFunction()) ? String.format(SqlConstants.FUNCTION_FOR_STRING_TYPE_TEMPLATE,
                    ipType.getCkValueFunction(), value) : SqlUtils.wrapFiledNameSingleQuota(value.toString());
        } else {
            return StringUtils.isNotBlank(ipType.getCkFiledFunction())
                    ? String.format(SqlConstants.FUNCTION_TEMPLATE, ipType.getCkFiledFunction(), value)
                    : value.toString();
        }
    }

    public static String getSelectTemplate(String selectSql, String table) {
        return String.format("SELECT %s FROM %s", selectSql, table);
    }

    /**
     * 是否为DateTime64时间类型.
     *
     * @param ckFieldType ck字段类型
     * @return true:是DateTime64类型
     */
    public static boolean isDateTime64(String ckFieldType) {
        return StringUtils.startsWith(ckFieldType, SqlConstants.TYPE_DATETIME64);
    }

    /**
     * DateTime64时间类型，提取精度.
     *
     * @param ckFieldType ck字段类型,如DateTime64(3) DateTime64(3, 'Asia/Shanghai')
     * @return 精度, 如3
     */
    public static int getDateTime64Scale(String ckFieldType) {
        String scale = ckFieldType.replace(SqlConstants.TYPE_DATETIME64, StringUtils.EMPTY)
                .replace(Constants.Symbol.LEFT_PARENTHESIS, StringUtils.EMPTY)
                .replace(Constants.Symbol.RIGHT_PARENTHESIS, StringUtils.EMPTY);
        if (includeTimezone(scale)) {
            scale = scale.split(Constants.Symbol.COMMA_QUOTA)[0];
        }
        return Integer.parseInt(scale);
    }

    /**
     * 是否包含时区.
     *
     * @param scale 当前类型
     * @return 是否为时区类型
     */
    private static boolean includeTimezone(String scale) {
        return scale.contains(Constants.Symbol.COMMA_QUOTA);
    }

    /**
     * 是否为DateTime时间类型.
     *
     * @param ckFieldType ck字段类型
     * @return true:是DateTime类型
     */
    public static boolean isDateTime(String ckFieldType) {
        return StringUtils.startsWith(ckFieldType, SqlConstants.TYPE_DATETIME);
    }

}
