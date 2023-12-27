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
package com.ly.ckibana.converter;

import com.ly.ckibana.CommonTest;
import org.junit.Test;

/**
 * 测试DSL查询，基于Discover测试
 *
 * @author zl11357
 * @since 2023/10/21 21:00
 */
public class CommonDSLTest extends CommonTest {
    public static final String TEST_INTEGER_DSL = "INTEGER_DSL";
    public static final String TEST_STRING_DSL = "STRING_DSL";
    public static final String TEST_DATETIME64_DSL = "DATETIME64_DSL";

    /**
     * 整型DSL查询测试
     */
    @Test
    public void testIntegerDSL() {
        String query = "{\"version\":true,\"size\":500,\"sort\":[{\"@timestamp\":{\"order\":\"desc\",\"unmapped_type\":\"boolean\"}}],\"_source\":{\"excludes\":[]},\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1}}},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_phrase\":{\"i1\":{\"query\":1}}},{\"bool\":{\"should\":[{\"match_phrase\":{\"i1\":\"3\"}},{\"match_phrase\":{\"i1\":\"4\"}}],\"minimum_should_match\":1}},{\"range\":{\"i1\":{\"gte\":7,\"lt\":8}}},{\"exists\":{\"field\":\"i1\"}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697892216652,\"lte\":1697893116652,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[{\"match_phrase\":{\"i1\":{\"query\":2}}},{\"bool\":{\"should\":[{\"match_phrase\":{\"i1\":\"5\"}},{\"match_phrase\":{\"i1\":\"6\"}}],\"minimum_should_match\":1}},{\"range\":{\"i1\":{\"gte\":9,\"lt\":10}}},{\"exists\":{\"field\":\"i2\"}}]}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE ((`i1` = 1) AND ((`i1` = 3) OR (`i1` = 4)) AND (  `i1` < 8  AND  `i1` >= 7  ) AND (isNotNull(`i1`))) AND ( NOT (`i1` = 2) AND NOT ((`i1` = 5) OR (`i1` = 6)) AND NOT (  `i1` < 10  AND  `i1` >= 9  ) AND NOT (isNotNull(`i2`))) AND ((  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697893110000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697892210000  )) GROUP BY `2_@timestampDateTime` ORDER BY `2_@timestampDateTime` asc LIMIT 30000\",\n" +
                "                \"SELECT * FROM `table1_all` PREWHERE ((`i1` = 1) AND ((`i1` = 3) OR (`i1` = 4)) AND (  `i1` < 8  AND  `i1` >= 7  ) AND (isNotNull(`i1`))) AND ( NOT (`i1` = 2) AND NOT ((`i1` = 5) OR (`i1` = 6)) AND NOT (  `i1` < 10  AND  `i1` >= 9  ) AND NOT (isNotNull(`i2`))) AND ((  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697893110000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697892210000  )) ORDER BY `@timestamp` DESC LIMIT 500\"\n" +
                "            ]";
        doTest(TEST_INTEGER_DSL, query, Boolean.TRUE, expectedSqls);

    }

    /**
     * 字符串类型DSL查询测试
     */
    @Test
    public void testStringDSL() {
        String query = "{\"version\":true,\"size\":500,\"sort\":[{\"@timestamp\":{\"order\":\"desc\",\"unmapped_type\":\"boolean\"}}],\"_source\":{\"excludes\":[]},\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1}}},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_phrase\":{\"s1\":{\"query\":\"sv1\"}}},{\"bool\":{\"should\":[{\"match_phrase\":{\"s1\":\"sv3\"}},{\"match_phrase\":{\"s1\":\"sv4\"}}],\"minimum_should_match\":1}},{\"exists\":{\"field\":\"s1\"}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697892781326,\"lte\":1697893681326,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[{\"match_phrase\":{\"s1\":{\"query\":\"sv2\"}}},{\"bool\":{\"should\":[{\"match_phrase\":{\"s1\":\"sv5\"}},{\"match_phrase\":{\"s1\":\"sv6\"}}],\"minimum_should_match\":1}},{\"exists\":{\"field\":\"s2\"}}]}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647},\"timeout\":\"120000ms\"}\n";
        String expectedSqls = "[\n" +
                "                \"SELECT toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE ((`s1` like '%sv1%') AND ((`s1` like '%sv3%') OR (`s1` like '%sv4%')) AND (isNotNull(`s1`) AND `s1` != '')) AND ( NOT (`s1` like '%sv2%') AND NOT ((`s1` like '%sv5%') OR (`s1` like '%sv6%')) AND NOT (isNotNull(`s2`) AND `s2` != '')) AND ((  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697893680000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697892780000  )) GROUP BY `2_@timestampDateTime` ORDER BY `2_@timestampDateTime` asc LIMIT 30000\",\n" +
                "                \"SELECT * FROM `table1_all` PREWHERE ((`s1` like '%sv1%') AND ((`s1` like '%sv3%') OR (`s1` like '%sv4%')) AND (isNotNull(`s1`) AND `s1` != '')) AND ( NOT (`s1` like '%sv2%') AND NOT ((`s1` like '%sv5%') OR (`s1` like '%sv6%')) AND NOT (isNotNull(`s2`) AND `s2` != '')) AND ((  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697893680000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697892780000  )) ORDER BY `@timestamp` DESC LIMIT 500\"\n" +
                "            ]";
        doTest(TEST_STRING_DSL, query, Boolean.TRUE, expectedSqls);

    }

    /**
     * DateTime64类型DSL
     */
    @Test
    public void testTimeFieldDSL() {
        String query = "{\"version\":true,\"size\":500,\"sort\":[{\"@timestamp\":{\"order\":\"desc\",\"unmapped_type\":\"boolean\"}}],\"_source\":{\"excludes\":[]},\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1}}},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_phrase\":{\"@timestampDateTime\":{\"query\":\"2023-10-23 21:05:30.000\"}}},{\"bool\":{\"minimum_should_match\":1,\"should\":[{\"match_phrase\":{\"@timestampDateTime\":\"2023-10-23 20:02:00.000\"}},{\"match_phrase\":{\"@timestampDateTime\":\"2023-10-23 20:03:00.000\"}}]}},{\"range\":{\"@timestampDateTime\":{\"gte\":\"2023-10-23 20:06:00.000\",\"lt\":\"2023-10-23 20:07:00.000\"}}},{\"exists\":{\"field\":\"@timestampDateTime\"}},{\"range\":{\"@timestampDateTime\":{\"gte\":1698066374405,\"lte\":1698067274405,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[{\"match_phrase\":{\"@timestampDateTime\":{\"query\":\"2023-10-23 20:01:00.000\"}}},{\"bool\":{\"minimum_should_match\":1,\"should\":[{\"match_phrase\":{\"@timestampDateTime\":\"2023-10-23 20:04:00.000\"}},{\"match_phrase\":{\"@timestampDateTime\":\"2023-10-23 20:05:00.000\"}}]}},{\"range\":{\"@timestampDateTime\":{\"gte\":\"2023-10-23 20:08:00.000\",\"lt\":\"2023-10-23 20:09:00.000\"}}}]}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647},\"timeout\":\"300000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE ((`@timestampDateTime` = '2023-10-23 21:05:30.000') AND ((`@timestampDateTime` = '2023-10-23 20:02:00.000') OR (`@timestampDateTime` = '2023-10-23 20:03:00.000')) AND (isNotNull(`@timestampDateTime`))) AND ( NOT (`@timestampDateTime` = '2023-10-23 20:01:00.000') AND NOT ((`@timestampDateTime` = '2023-10-23 20:04:00.000') OR (`@timestampDateTime` = '2023-10-23 20:05:00.000'))) AND ((  toUnixTimestamp64Milli(`@timestampDateTime`) < 1698062940000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1698062880000  )) GROUP BY `2_@timestampDateTime` ORDER BY `2_@timestampDateTime` asc LIMIT 30000\",\n" +
                "                \"SELECT * FROM `table1_all` PREWHERE ((`@timestampDateTime` = '2023-10-23 21:05:30.000') AND ((`@timestampDateTime` = '2023-10-23 20:02:00.000') OR (`@timestampDateTime` = '2023-10-23 20:03:00.000')) AND (isNotNull(`@timestampDateTime`))) AND ( NOT (`@timestampDateTime` = '2023-10-23 20:01:00.000') AND NOT ((`@timestampDateTime` = '2023-10-23 20:04:00.000') OR (`@timestampDateTime` = '2023-10-23 20:05:00.000'))) AND ((  toUnixTimestamp64Milli(`@timestampDateTime`) < 1698062940000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1698062880000  )) ORDER BY `@timestamp` DESC LIMIT 500\"\n" +
                "            ]";
        doTest(TEST_DATETIME64_DSL, query, Boolean.TRUE, expectedSqls);
    }

    /**
     * Ipv4 Ipv6类型DSL
     */
    @Test
    public void testIpv4OrIpv6DSL() {
        String query = "{\"version\":true,\"size\":500,\"sort\":[{\"@timestamp\":{\"order\":\"desc\",\"unmapped_type\":\"boolean\"}}],\"_source\":{\"excludes\":[]},\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1}}},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_phrase\":{\"ipv4\":{\"query\":\"1.1.1.1\"}}},{\"bool\":{\"should\":[{\"match_phrase\":{\"ipv4\":\"3.3.3.3\"}},{\"match_phrase\":{\"ipv4\":\"4.4.4.4\"}}],\"minimum_should_match\":1}},{\"range\":{\"ipv4\":{\"gte\":\"7.7.7.7\",\"lt\":\"8.8.8.8\"}}},{\"exists\":{\"field\":\"ipv6\"}},{\"range\":{\"@timestampDateTime\":{\"gte\":1698056661828,\"lte\":1698057561828,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[{\"match_phrase\":{\"ipv4\":{\"query\":\"2.2.2.2\"}}},{\"bool\":{\"should\":[{\"match_phrase\":{\"ipv4\":\"5.5.5.5\"}},{\"match_phrase\":{\"ipv4\":\"6.6.6.6\"}}],\"minimum_should_match\":1}},{\"range\":{\"ipv4\":{\"gte\":\"9.9.9.9\",\"lt\":\"10.10.10.10\"}}},{\"exists\":{\"field\":\"ipv4\"}}]}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE ((`ipv4` = IPv4StringToNumOrDefault('1.1.1.1')) AND ((`ipv4` = IPv4StringToNumOrDefault('3.3.3.3')) OR (`ipv4` = IPv4StringToNumOrDefault('4.4.4.4'))) AND (  `ipv4` < IPv4StringToNumOrDefault('8.8.8.8')  AND  `ipv4` >= IPv4StringToNumOrDefault('7.7.7.7')  ) AND (isNotNull(`ipv6`))) AND ( NOT (`ipv4` = IPv4StringToNumOrDefault('2.2.2.2')) AND NOT ((`ipv4` = IPv4StringToNumOrDefault('5.5.5.5')) OR (`ipv4` = IPv4StringToNumOrDefault('6.6.6.6'))) AND NOT (  `ipv4` < IPv4StringToNumOrDefault('10.10.10.10')  AND  `ipv4` >= IPv4StringToNumOrDefault('9.9.9.9')  ) AND NOT (isNotNull(`ipv4`))) AND ((  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1698057560000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1698056660000  )) GROUP BY `2_@timestampDateTime` ORDER BY `2_@timestampDateTime` asc LIMIT 30000\",\n" +
                "                \"SELECT * FROM `table1_all` PREWHERE ((`ipv4` = IPv4StringToNumOrDefault('1.1.1.1')) AND ((`ipv4` = IPv4StringToNumOrDefault('3.3.3.3')) OR (`ipv4` = IPv4StringToNumOrDefault('4.4.4.4'))) AND (  `ipv4` < IPv4StringToNumOrDefault('8.8.8.8')  AND  `ipv4` >= IPv4StringToNumOrDefault('7.7.7.7')  ) AND (isNotNull(`ipv6`))) AND ( NOT (`ipv4` = IPv4StringToNumOrDefault('2.2.2.2')) AND NOT ((`ipv4` = IPv4StringToNumOrDefault('5.5.5.5')) OR (`ipv4` = IPv4StringToNumOrDefault('6.6.6.6'))) AND NOT (  `ipv4` < IPv4StringToNumOrDefault('10.10.10.10')  AND  `ipv4` >= IPv4StringToNumOrDefault('9.9.9.9')  ) AND NOT (isNotNull(`ipv4`))) AND ((  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1698057560000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1698056660000  )) ORDER BY `@timestamp` DESC LIMIT 500\"\n" +
                "            ]";
        doTest(TEST_DATETIME64_DSL, query, Boolean.TRUE, expectedSqls);

    }
}
