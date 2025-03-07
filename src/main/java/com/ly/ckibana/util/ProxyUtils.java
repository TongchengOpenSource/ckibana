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

import com.google.common.base.Strings;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.aggregation.Interval;
import com.ly.ckibana.model.enums.IPType;
import com.ly.ckibana.model.request.CkRequest;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.response.Hits;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.model.response.Shards;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

import java.util.*;

@Slf4j
public class ProxyUtils {

    public static final long MILLISECOND = 1;

    public static final String TIME_FIELD_OPTION_MATCH_NAME = "time";

    private static final Map<String, Long> TIME_UNIT_MAP = new LinkedHashMap<>();

    private static final List<String> NUMBER_TYPE = Arrays.asList("UInt8", "UInt16", "UInt32", "UInt64",
            "Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "Decimal");

    /**
     * 从es的interval str（30s）中解析数字和单位（30,s）.
     * 本系统默认时间戳到 MICROSECOND
     */
    public static Interval parseInterval(String intervalStr) {
        Interval result = null;
        if (TIME_UNIT_MAP.isEmpty()) {
            TIME_UNIT_MAP.put("ms", MILLISECOND);
            TIME_UNIT_MAP.put("s", 1000 * MILLISECOND);
            TIME_UNIT_MAP.put("m", 60 * 1000 * MILLISECOND);
            TIME_UNIT_MAP.put("h", 60 * 60 * 1000 * MILLISECOND);
            TIME_UNIT_MAP.put("d", 24 * 60 * 60 * 1000 * MILLISECOND);
            TIME_UNIT_MAP.put("w", 7 * 24 * 60 * 60 * 1000 * MILLISECOND);
        }
        for (Map.Entry<String, Long> each : TIME_UNIT_MAP.entrySet()) {
            if (intervalStr.endsWith(each.getKey())) {
                result = new Interval();
                result.setTimeUnit(each.getValue());
                result.setValue(Integer.parseInt(Utils.trimSuffix(intervalStr, each.getKey())));
                break;
            }
        }
        return result;
    }

    /**
     * ck基础类型,排查封装类型.
     *
     * @param type ck类型
     * @return 基础类型
     */
    public static String parseCkBaseType(String type) {
        String result = Utils.trimPrefix(type, "Array");
        result = Utils.trimPrefix(result, "Nullable");
        result = Utils.trimPrefix(result, "LowCardinality");
        result = Utils.trimByPrefixAndSuffixChars(result, "(", ")");
        return result;
    }

    /**
     * 值转换。若时间字段的utc时间和gmt时间转换为时间戳参数.
     *
     * @param ckFieldType   ck字段类型
     * @param ckFieldName   ck字段名
     * @param value         值
     * @param timeFieldName 时间字段
     * @return 转换后的值
     */
    public static Object convertValue(String ckFieldType, String ckFieldName, Object value, String timeFieldName) {
        Object result = value;
        if (ProxyUtils.isString(ckFieldType) && StringUtils.equals(timeFieldName, ckFieldName)) {
            Object timeValue = DateUtils.toEpochMilli(result.toString());
            if (timeValue != null) {
                result = timeValue;
            }
        }

        return result;
    }

    /**
     * 解析ck字段类型,扩展字段和和默认字段为String类型.
     */
    public static String getCkFieldTypeByName(String ckFieldName, Map<String, String> columns) {
        if (columns.containsKey(ckFieldName)) {
            return columns.get(ckFieldName);
        } else if (!checkIfExtensionInnerField(ckFieldName)) {
            log.error("unknown ck field: {}, columns:{}", ckFieldName, columns);
        }
        return "String";
    }

    /**
     * ck类型是否为数值类.
     */
    public static boolean isNumeric(String type) {
        return NUMBER_TYPE.contains(parseCkBaseType(type));
    }

    /**
     * ck类型是否为float或decimal类型.
     */
    public static boolean isDouble(String type) {
        return isNumeric(type) && (SqlConstants.TYPE_FLOAT.equals(parseCkBaseType(type))
                || SqlConstants.TYPE_DECIMAL.equals(parseCkBaseType(type)));
    }

    /**
     * ck类型数值类型转为es识别的类型.
     */
    public static String convertCkNumberTypeToEsType(String type) {
        String ckBaseType = parseCkBaseType(type);
        if (ckBaseType.endsWith("Int8")) {
            return "byte";
        } else if (ckBaseType.endsWith("Int16")) {
            return "short";
        } else if (ckBaseType.endsWith("Int32")) {
            return "integer";
        } else if (ckBaseType.endsWith("Int64")) {
            return "long";
        } else if (ckBaseType.startsWith("Float")) {
            return "float";
        } else {
            return "double";
        }
    }

    /**
     * 基于值正则或基于ck字段类型判断是否为IPV4类查询.
     * 目前仅支持 string类型ipv4
     */
    public static IPType getIpType(String ckFieldType, String value) {
        IPType ipType = null;
        if (IPType.IPV4.getCkType().equals(ckFieldType)) {
            ipType = IPType.IPV4;
        } else if (IPType.IPV6.getCkType().equals(ckFieldType)) {
            ipType = IPType.IPV6;
        } else if (StringUtils.isNotBlank(value)) {
            if (Utils.isIPv4Value(value) && IPType.IPV4_STRING.getCkType().equals(ckFieldType)) {
                ipType = IPType.IPV4_STRING;
            } else if (Utils.isIPv6Value(value) && IPType.IPV6_STRING.getCkType().equals(ckFieldType)) {
                ipType = IPType.IPV6_STRING;
            }
        }
        return ipType;
    }

    /**
     * 是否为数组类字段，目前暂未实际使用.
     * 基于数据类型决定转换语法 https://clickhouse.tech/docs/zh/sql-reference/data-types/
     */
    public static boolean isArrayType(String type) {
        return type.startsWith("Array");
    }

    /**
     * 是否为字符串类型字段.
     */
    public static boolean isString(String type) {
        return SqlConstants.TYPE_STRING.equals(parseCkBaseType(type));
    }

    /**
     * 是否为date类型字段.
     */
    public static boolean isDate(String type) {
        return parseCkBaseType(type).startsWith("Date");
    }

    /**
     * 构建kibana exception.
     *
     * @param error 异常说明
     * @return Response
     */
    public static Response newKibanaException(String error) {
        Map<String, Object> searchError = new HashMap<>(2, 1);
        searchError.put("name", "SearchError");
        searchError.put("message", error);
        Response result = new Response();
        result.setAggregations(null);
        result.setHits(null);
        result.setShards(null);
        result.setError(searchError);
        return result;
    }

    public static Map<String, Object> newKibanaExceptionV8(String error) {
        return Map.of(
                "response", Map.of(
                        "hits", new Hits(),
                        "_shards", new Shards()
                ),
                "error", Map.of(
                        "type", "status_exception",
                        "reason", error
                ));
    }

    /**
     * 构建kibana exception.
     *
     * @param httpStatus 状态码
     * @param error      异常说明
     * @return Response
     */
    public static Response newKibanaException(HttpStatus httpStatus, String error) {
        Map<String, Object> searchError = new HashMap<>(2, 1);
        searchError.put("name", "SearchError");
        searchError.put("message", error);
        Response result = new Response();
        result.setStatus(httpStatus.value());
        result.setAggregations(null);
        result.setHits(null);
        result.setShards(null);
        result.setError(searchError);
        return result;
    }

    public static String getErrorResponse(Exception e) {
        return getErrorResponse(e.getMessage());
    }

    public static String getErrorResponse(String message) {
        return String.format("{\"status\":400, \"error\":\"%s\"}", message);
    }

    /**
     * 时间查询sql转换，支持字符串（耗性能，不推荐）和数值类型,DateTime, DateTime64类型.
     */
    public static String generateTimeFieldSqlWithFormatUnixTimestamp64(String ckFieldName, String ckFieldType) {
        if (SqlUtils.isDateTime64(ckFieldType)) {
            return String.format("toUnixTimestamp64Milli(%s)", getFieldSqlPart(ckFieldName));
        } else if (SqlUtils.isDateTime(ckFieldType)) {
            return String.format("toUnixTimestamp(%s)*1000", getFieldSqlPart(ckFieldName));
        } else {
            return getFieldSqlPart(ckFieldName);
        }
    }

    /**
     * range请求包装。用于：将不支持range的原始字段转换为支持range的ck function包装后的值.
     * 时间字段：支持数值型timestamp 和DateTime, DateTime64存储类型
     * ip字段：支持字符串存储类型=> 利用IPv4StringToNum()语法实现range
     * 普通数值字段：支持数值型存储类型=> 无需额外转换
     *
     * @param isTimeField 是否为indexPattern的时间字段
     */
    public static Range getRangeWrappedBySqlFunction(Range orgRange, boolean isTimeField) {
        String ckFieldName = orgRange.getCkFieldName();
        String ckFieldType = orgRange.getCkFieldType();
        Range rangeConverted = new Range(ckFieldName, orgRange.getCkFieldType(), orgRange.getHigh(), orgRange.getLow());
        if (isTimeField && !isNumeric(orgRange.getCkFieldType())) {
            //时间字段且为非数值类型
            rangeConverted.setCkFieldName(ckFieldName);
            rangeConverted.setHigh(generateTimeFieldSqlWithFormatDateTime64ZoneShangHai(orgRange.getHigh(), ckFieldType));
            rangeConverted.setLow(generateTimeFieldSqlWithFormatDateTime64ZoneShangHai(orgRange.getLow(), ckFieldType));
        } else {
            //ip
            IPType ipType = ProxyUtils.getIpType(orgRange.getCkFieldType(), orgRange.getLow().toString());
            if (null != ipType) {
                rangeConverted.setCkFieldName(SqlUtils.generateIpSql(ckFieldName, ipType, false));
                rangeConverted.setHigh(SqlUtils.generateIpSql(orgRange.getHigh(), ipType, true));
                rangeConverted.setLow(SqlUtils.generateIpSql(orgRange.getLow(), ipType, true));
            }

        }
        return rangeConverted;
    }

    /**
     * 时间字段转换。作为值 or 字段.
     */
    public static String generateTimeFieldSqlWithFormatDateTime64ZoneShangHai(Object value, String ckFieldType) {
        if (SqlUtils.isDateTime64(ckFieldType)) {
            return String.format("toDateTime64(%s/1000,%d)", value.toString(), SqlUtils.getDateTime64Scale(ckFieldType));
        } else if (SqlUtils.isDateTime(ckFieldType)) {
            return String.format("toDateTime(%s/1000)", value.toString());
        } else {
            return value.toString();
        }
    }

    public static Object trimNull(Object body) {
        return null == body || Strings.isNullOrEmpty(body.toString()) ? "" : body.toString();
    }

    /**
     * 代理自定义语法 忽略大小写.
     */
    public static boolean isCaseIgnoreQuery(String field) {
        return field.endsWith(Constants.UI_PHRASE_CASE_IGNORE);
    }

    /**
     * 代理自定义语法 忽略大小写.
     */
    public static boolean isCaseIgnoreKeywordStringQuery(String field) {
        return field.endsWith(Constants.UI_PHRASE_CASE_IGNORE + Constants.ES_KEYWORD);
    }

    /**
     * ck类型转为es类型，供kibana 识别.
     */
    public static String convertCkTypeToEsType(String ckName, String ckType, boolean isForSelectTimeField) {
        String result = "string";
        if (null != getIpType(ckType, StringUtils.EMPTY)) {
            result = "ip";
        } else if ("_source".equals(ckName)) {
            result = "_source";
        } else if (isDate(ckType) || isTimeFieldOption(ckName, ckType, isForSelectTimeField)) {
            result = "date";
        } else if (isString(ckType)) {
            result = "string";
        } else if (isNumeric(ckType)) {
            result = convertCkNumberTypeToEsType(ckType);
        }
        return result;
    }

    /**
     * 除了DateTime64类型，其他可作为时间字段的字段列表。如采样时用的UInt类型等.
     */
    private static boolean isTimeFieldOption(String ckName, String ckType, boolean isForSelectTimeField) {
        return isForSelectTimeField && (ckName.contains(TIME_FIELD_OPTION_MATCH_NAME) || isNumeric(ckType));
    }

    /**
     * ui字段名 转为es字段名.
     */
    public static String convertUiFieldToEsField(String orgField) {
        String result = orgField;
        if (isCaseIgnoreQuery(orgField) || isCaseIgnoreKeywordStringQuery(orgField)) {
            result = result.replace(Constants.UI_PHRASE_CASE_IGNORE, StringUtils.EMPTY);
        }
        result = result.replace(Constants.ES_KEYWORD, StringUtils.EMPTY);
        return result;
    }

    /**
     * get no symbol value name.
     * 'A' -> A
     * (A) -> A
     * "A" -> A
     *
     * @param confusedValue 混淆的值
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/26 20:30
     */
    public static String getPurValue(String confusedValue) {
        String pureColumnName = confusedValue.replace(Constants.Symbol.DOUBLE_QUOTA, StringUtils.EMPTY);
        pureColumnName = pureColumnName.replace(Constants.Symbol.SINGLE_QUOTA, StringUtils.EMPTY);
        pureColumnName = pureColumnName.replace(Constants.Symbol.LEFT_PARENTHESIS, StringUtils.EMPTY);
        pureColumnName = pureColumnName.replace(Constants.Symbol.RIGHT_PARENTHESIS, StringUtils.EMPTY);
        return StringUtils.trim(pureColumnName);
    }

    /**
     * get no symbol column name.
     * A.keyword => A
     * A.caseIgnore => A
     * A.caseIgnore.keyword => A
     *
     * @param confusedColumnName ui语法
     * @return java.lang.String
     */
    public static String getPureColumnName(String confusedColumnName) {
        String pureColumnName = confusedColumnName.replace(Constants.UI_PHRASE_CASE_IGNORE, StringUtils.EMPTY);
        pureColumnName = pureColumnName.replace(Constants.ES_KEYWORD, StringUtils.EMPTY);
        return StringUtils.trim(pureColumnName);
    }

    /**
     * 是否为创建indexpattern的页面初始查询.
     */
    public static boolean matchAllIndex(String index) {
        return Constants.INDEX_PATTERN_ALL.contains(index);
    }

    /**
     * 是否为查询extension内动态字段.
     */
    public static boolean checkIfExtensionInnerField(String ckField) {
        return ckField.startsWith(Constants.CK_EXTENSION_QUERY_FUNCTION);
    }

    /**
     * 将字段名转换为sql中拼接的格式，添加符转义或json内解析函数.
     */
    public static String getFieldSqlPart(String ckField) {
        String fieldExpr = (checkIfExtensionInnerField(ckField) || ckField.startsWith(Constants.Symbol.BACK_QUOTE_CHAR)) ? "%s" : "`%s`";
        return String.format(fieldExpr, ckField);
    }

    /**
     * es索引转换为表名.
     * 表名规范替换-换成_
     */
    public static CkRequest buildCkRequest(CkRequestContext ckRequestContext) {
        String ckTableName = ckRequestContext.getTableName();
        return buildRequest(ckTableName, "*");
    }

    public static CkRequest buildRequest(String table, String... selectParam) {
        CkRequest result = new CkRequest();
        String escapeTable = table;
        if (!escapeTable.startsWith("`")) {
            escapeTable = SqlUtils.escape(escapeTable);
        }
        result.setTable(escapeTable);
        if (selectParam.length > 0) {
            result.setSelect(selectParam[0]);
        }
        return result;
    }

    public static boolean isPreciseField(String field) {
        return field.endsWith(Constants.ES_KEYWORD);
    }

    public static boolean isRangeQuery(String condition) {
        return condition.startsWith(Constants.Symbol.LEFT_PARENTHESIS) && condition.endsWith(Constants.Symbol.RIGHT_PARENTHESIS);
    }

    public static String trimRemoteCluster(String originalIndex) {
        // trim remote cluster
        if (!StringUtils.isEmpty(originalIndex) && originalIndex.contains(Constants.QUERY_CLUSTER_SPLIT)) {
            return originalIndex.substring(originalIndex.indexOf(Constants.QUERY_CLUSTER_SPLIT) + 1);
        }
        return originalIndex;
    }

    /**
     * 是否为创建index pattern的初始*请求
     *
     * @param index
     * @return
     */
    public static boolean isWildcardIndexPattern(String index) {
        return index.equals(Constants.MATCH_ALL);
    }
}
