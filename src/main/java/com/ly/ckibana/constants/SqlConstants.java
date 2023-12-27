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
package com.ly.ckibana.constants;

import java.util.List;

public class SqlConstants {
    
    /**
     * SQL format.
     */
    public static final String PREWHERE_TEMPLATE = "PREWHERE %s";

    public static final String GROUP_BY_TEMPLATE = "GROUP BY %s";

    public static final String ORDER_BY_TEMPLATE = "ORDER BY %s";

    public static final String LIMIT_TEMPLATE = "LIMIT %d";

    public static final String COUNT_QUERY = "count(1) as _count";

    public static final String TIME_AGG_BY_MINUTE_TEMPLATE = "intDiv(%s,1000*60) as %s";

    public static final String FUNCTION_TEMPLATE = "%s(%s)";

    public static final String FUNCTION_FOR_STRING_TYPE_TEMPLATE = "%s('%s')";

    public static final String FUNCTION_FOR_CASE_INSENSITIVE = "(positionCaseInsensitive(%s, '%s') != 0)";

    public static final String FUNCTION_TO_LOWER = "lower('%s')";

    /**
     * 聚合中使用的字段变量.
     */
    public static final String DEFAULT_COUNT_NAME = "_count";

    public static final String CK_COUNT_NAME = "_ckCount";

    public static final String CK_MINUTE_NAME = "_ckMinute";

    public static final String DATE_TIME = "date_time";

    public static final String FIELD = "field";

    public static final String FORMAT = "format";

    public static final String KEY_NAME = "_key";

    public static final String ORDER = "order";

    public static final String GTE_NAME = "gte";

    public static final String GT_NAME = "gt";

    public static final String LT_NAME = "lt";

    public static final String LTE_NAME = "lte";

    public static final String AND = "AND";

    public static final String MIN = "MIN";

    public static final String MAX = "MAX";

    public static final String SUM = "SUM";

    public static final String AVG = "avg";

    public static final String TO_INT64 = "toInt64";

    public static final String DISTINCT = "distinct";

    public static final String COUNT_IF = "countIf";

    public static final String COUNT = "count";

    public static final String TO = "TO";

    public static final String OR = "OR";
    
    /**
     * CK字段类型.
     */
    public static final String TYPE_STRING = "String";

    public static final String TYPE_DATETIME64 = "DateTime64";

    public static final String TYPE_FLOAT = "Float";

    public static final String TYPE_DECIMAL = "Decimal";

    public static final String SYSTEM_TABLE = "system.tables";
    
    public static final Integer DEFAULT_SORT_BY_SUB_AGG_INDEX = -1;
    
    public static final List<String> LOGICAL_OPERATOR = List.of("and", "or", "xor", "not", "and not");

    public static final String QUERY_NAME_SEPARATOR = "_";
}
