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
import java.util.Map;

public class Constants {

    public static final String HEADER_ELASTICSEARCH = "Elasticsearch";

    public static final String HEADER_X_ELASTIC_PRODUCT = "X-elastic-product";

    public static final String DATE_FORMAT_DEFAULT = "yyyy-MM-dd";

    public static final String DATETIME_FORMAT_DEFAULT = "yyyy-MM-dd HH:mm:ss";

    public static final String DATETIME_FORMAT_YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd HH:mm:ss.SSS";

    public static final String DATETIME_FORMAT_GMT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final String DATETIME_FORMAT_GMT_PLUS_EIGHT_HOUR = "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00";

    public static final List<String> EXTENDED_DATETIME_FORMAT_LIST = List.of(DATE_FORMAT_DEFAULT, DATETIME_FORMAT_DEFAULT,
            DATETIME_FORMAT_YYYY_MM_DD_HH_MM_SS_SSS, DATETIME_FORMAT_GMT, DATETIME_FORMAT_GMT_PLUS_EIGHT_HOUR);

    public static final String KIBANA_META_INDEX = ".kibana,.kibana_analytics_*";

    public static final String X_REQUEST_ID = "x-request-id";

    public static final String INDEX_NAME_KEY = "index";

    public static final String KEY_NAME = "key";

    /**
     * es查询参数关键字.
     */
    public static final String SIZE = "size";

    public static final String SORT = "sort";

    public static final String DOC_VALUE_FIELDS = "docvalue_fields";

    public static final String QUERY = "query";

    public static final String AGGS = "aggs";

    public static final String RANGE_FROM = "from";

    public static final String RANGE_TO = "to";

    public static final String RANGES = "ranges";

    public static final String PERCENTS = "percents";

    public static final String VALUES = "values";

    public static final String FIELD = "field";

    public static final String INTERVAL = "interval";

    public static final String FIXED_INTERVAL = "fixed_interval";

    public static final String CALENDAR_INTERVAL = "calendar_interval";

    public static final String MATCH_ALL = "*";

    public static final String ES_KEYWORD = ".keyword";

    public static final String FORMAT = "format";

    public static final String SOURCE = "_source";

    public static final String HIT = "hits";

    public static final String INDEX_PATTERN = "index-pattern";

    /**
     * 索引中集群分隔符.
     */
    public static final String QUERY_CLUSTER_SPLIT = ":";

    public static final int AGG_INIT_DEPTH = 1;

    public static final String RANGE_SPLIT = "-";

    /**
     * 额外字段,可考虑是否需要.
     */
    public static final String ES_INDEX_QUERY_FIELD = "_index";

    /**
     * 扩展字段.
     */
    public static final String CK_EXTENSION = "ck_assembly_extension";

    public static final String CK_EXTENSION_QUERY_FUNCTION = "JSONExtractString";

    public static final List<String> INDEX_PATTERN_ALL = List.of("*", "*:*");

    public static final int DEFAULT_NO_SAMPLE = 1;

    public static final Long DEFAULT_TOTAL_COUNT = 0L;


    /**
     * 时间round.
     */
    public static final int ROUND_SECOND = 10;

    public static final long USE_SAMPLE_COUNT_THREASHOLD = 0;

    public static final int CK_NUMBER_DEFAULT_VALUE = Integer.MAX_VALUE;

    /**
     * 忽略大小写自定义语法:兼容原es keyword忽略大小写功能.
     */
    public static final String UI_PHRASE_CASE_IGNORE = ".caseIgnore";

    public static final String CK_LIKE_SPLIT = "%";

    public static class Symbol {

        public static final String BACK_QUOTE_CHAR = "`";

        public static final String SINGLE_SPACE_STRING = " ";

        public static final char SINGLE_SPACE_CHAR = ' ';

        public static final char LEFT_PARENTHESIS_CHAR = '(';

        public static final char RIGHT_PARENTHESIS_CHAR = ')';

        public static final char COLON_CHAR = ':';

        public static final char LEFT_BRACKET_CHAR = '[';

        public static final char RIGHT_BRACKET_CHAR = ']';

        public static final String GTE = ">=";

        public static final String GT = ">";

        public static final String LT = "<";

        public static final String LTE = "<=";

        public static final String LEFT_PARENTHESIS = "(";

        public static final String RIGHT_PARENTHESIS = ")";

        public static final String SINGLE_QUOTA = "'";

        public static final String DOUBLE_QUOTA = "\"";

        public static final String COMMA_QUOTA = ",";

        public static final String LEFT_BRACKET = "[";

        public static final String RIGHT_BRACKET = "]";

        public static final String COLON = ":";
    }

    public static class ConfigFile {

        public static final String SETTINGS_INDEX_NAME = "proxy-settings";

        public static final int SETTINGS_INDEX_SHARDS = 2;

        public static final Map<String, Object> SETTINGS_PROPERTIES = Map.of(
                "timestamp", Map.of("type", "date"),
                "key", Map.of("type", "keyword"),
                "value", Map.of("type", "text")
        );

        public static final String CACHE_INDEX_NAME = "proxy-cache";

        public static final int CACHE_INDEX_SHARDS = 2;

        public static final Map<String, Object> CACHE_PROPERTIES = Map.of(
                "timestamp", Map.of("type", "date"),
                "key", Map.of("type", "keyword"),
                "value", Map.of("type", "text", "index", false)
        );

        public static final String BLACK_LIST_INDEX_NAME = "proxy-black-list";

        public static final int BLACK_LIST_INDEX_SHARDS = 2;

        public static final Map<String, Object> BLACK_LIST_PROPERTIES = Map.of(
                "timestamp", Map.of("type", "date"),
                "key", Map.of("type", "keyword")
        );

        public static final String MONITOR_INDEX_NAME = "proxy-monitor";

        public static final int MONITOR_INDEX_SHARDS = 2;

        public static final Map<String, Object> MONITOR_PROPERTIES = Map.of(
                "timestamp", Map.of("type", "date"),
                "range", Map.of("type", "long"),
                "key", Map.of("type", "keyword"),
                "startTime", Map.of("type", "date"),
                "endTime", Map.of("type", "date"),
                "cost", Map.of("type", "long")
        );
    }

    public static class IndexBuilder {
        public static final String BULK_INDEX_HEADER = "{ \"index\": { \"_index\" : \"%s\", \"_type\" : \"info\", \"_id\": \"%s\"}}";

        public static final String BULK_INDEX_NO_TYPE_HEADER = "{ \"index\": { \"_index\" : \"%s\", \"_id\": \"%s\"}}";
    }

    public static class Headers {
        public static final String AUTHORIZATION = "authorization";
    }
}
