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
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.enums.TermsAggOrderType;
import org.junit.Test;

import static com.ly.ckibana.converter.CommonAggTest.NAME_AGGREGATION;

/**
 * terms agg排序类型测试
 *
 * @author zl11357
 * @since 2023/10/19 14:43
 */
public class CommonTermsAggOrderypeTest extends CommonTest {
    public static final String TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_COUNT_DESC_BASE_ON_DATE_HISTOGRAM = getTestName(NAME_AGGREGATION,AggType.TERMS.name(),AggType.DATE_HISTOGRAM.name(),TermsAggOrderType.METRIC_COUNT.name(), " desc");
    public static final String TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_COUNT_ASC_BASE_ON_DATE_HISTOGRAM = getTestName(NAME_AGGREGATION,AggType.TERMS.name(),AggType.DATE_HISTOGRAM.name(),TermsAggOrderType.METRIC_COUNT.name(), " asc");
    public static final String TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_CUSTOM_DESC_BASE_ON_DATE_HISTOGRAM = getTestName(NAME_AGGREGATION,AggType.TERMS.name(),AggType.DATE_HISTOGRAM.name(),TermsAggOrderType.METRIC_CUSTOM.name(), " desc");
    public static final String TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_CUSTOM_ASC_BASE_ON_DATE_HISTOGRAM = getTestName(NAME_AGGREGATION,AggType.TERMS.name(),AggType.DATE_HISTOGRAM.name(),TermsAggOrderType.METRIC_CUSTOM.name(), " asc");
    public static final String TEST_DEMO_TERMS_AGG_ORDER_BY_ALPHABETICAL_DESC_BASE_ON_DATE_HISTOGRAM = getTestName(NAME_AGGREGATION,AggType.TERMS.name(),AggType.DATE_HISTOGRAM.name(),TermsAggOrderType.ALPHABETICAL.name(), " desc");
    public static final String TEST_DEMO_TERMS_AGG_ORDER_BY_ALPHABETICAL_ASC_BASE_ON_DATE_HISTOGRAM = getTestName(NAME_AGGREGATION,AggType.TERMS.name(),AggType.DATE_HISTOGRAM.name(),TermsAggOrderType.ALPHABETICAL.name(), " asc");
    public static final String TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_CUSTOM_DESC_PIE= getTestName(NAME_AGGREGATION,AggType.TERMS.name(),TermsAggOrderType.METRIC_CUSTOM.name(), " desc");

    /**
     * 默认
     * TermsAggOrderType.METRIC_COUNT.name() desc 时序图
     */
    @Test
    public void testTermsAggOrderTypeMetricCountDesc() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"3\":{\"terms\":{\"field\":\"s1\",\"size\":5,\"order\":{\"_count\":\"desc\"}}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697890081722,\"lte\":1697890981722,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT `s1` as `3_s1`,count(1) as `3__ckCount`,toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697890980000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697890080000  ) AND (`3_s1` is not null AND `3_s1` != '' ) GROUP BY `2_@timestampDateTime`,`3_s1` ORDER BY `2_@timestampDateTime` asc,`3__ckCount` desc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_COUNT_DESC_BASE_ON_DATE_HISTOGRAM, query, Boolean.FALSE, expectedSqls);
    }

    /**
     * TermsAggOrderType.METRIC_COUNT.name() asc 时序图
     */
    @Test
    public void testTermsAggOrderTypeMetricCountAsc() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"3\":{\"terms\":{\"field\":\"s1\",\"size\":5,\"order\":{\"_count\":\"asc\"}}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697890185768,\"lte\":1697891085768,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT `s1` as `3_s1`,count(1) as `3__ckCount`,toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697891080000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697890180000  ) AND (`3_s1` is not null AND `3_s1` != '' ) GROUP BY `2_@timestampDateTime`,`3_s1` ORDER BY `2_@timestampDateTime` asc,`3__ckCount` asc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_COUNT_ASC_BASE_ON_DATE_HISTOGRAM, query, Boolean.FALSE, expectedSqls);
    }

    /**
     * TermsAggOrderType.METRIC_CUSTOM.name() desc 时序图
     */
    @Test
    public void testTermsAggOrderTypeMetricCustomDesc() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"3\":{\"terms\":{\"field\":\"s1\",\"size\":5,\"order\":{\"1\":\"desc\"}},\"aggs\":{\"1\":{\"avg\":{\"field\":\"i1\"}}}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697890248187,\"lte\":1697891148187,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = " [\n" +
                "                \"SELECT avg(`i1`) as `1_i1`,`s1` as `3_s1`,count(1) as `3__ckCount`,toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697891140000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697890240000  ) AND (`3_s1` is not null AND `3_s1` != '' ) GROUP BY `2_@timestampDateTime`,`3_s1` ORDER BY `2_@timestampDateTime` asc,`1_i1` desc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_CUSTOM_DESC_BASE_ON_DATE_HISTOGRAM, query, Boolean.FALSE, expectedSqls);
    }

    /**
     * TermsAggOrderType.METRIC_CUSTOM.name() asc 时序图
     */
    @Test
    public void testTermsAggOrderTypeMetricCustomAsc() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"3\":{\"terms\":{\"field\":\"s1\",\"size\":5,\"order\":{\"1\":\"asc\"}},\"aggs\":{\"1\":{\"avg\":{\"field\":\"i1\"}}}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697890248187,\"lte\":1697891148187,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = " [\n" +
                "                \"SELECT avg(`i1`) as `1_i1`,`s1` as `3_s1`,count(1) as `3__ckCount`,toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697891140000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697890240000  ) AND (`3_s1` is not null AND `3_s1` != '' ) GROUP BY `2_@timestampDateTime`,`3_s1` ORDER BY `2_@timestampDateTime` asc,`1_i1` asc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_TERMS_AGG_ORDER_BY_METRIC_CUSTOM_ASC_BASE_ON_DATE_HISTOGRAM, query, Boolean.FALSE, expectedSqls);
    }
    /**
     * TermsAggOrderType.ALPHABETICAL desc 时序图
     */
    @Test
    public void testTermsAggOrderTypeAlphabetcalDesc() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"3\":{\"terms\":{\"field\":\"s1\",\"size\":5,\"order\":{\"_key\":\"desc\"}}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697769023230,\"lte\":1697769923230,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT `s1` as `3_s1`,count(1) as `3__ckCount`,toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697769920000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697769020000  ) AND (`3_s1` is not null AND `3_s1` != '' ) GROUP BY `2_@timestampDateTime`,`3_s1` ORDER BY `2_@timestampDateTime` asc,`s1` desc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_TERMS_AGG_ORDER_BY_ALPHABETICAL_DESC_BASE_ON_DATE_HISTOGRAM, query, Boolean.FALSE, expectedSqls);
    }
    /**
     * TermsAggOrderType.ALPHABETICAL asc 时序图
     */
    @Test
    public void testTermsAggOrderTypeAlphabetcalAsc() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"3\":{\"terms\":{\"field\":\"s1\",\"size\":5,\"order\":{\"_key\":\"asc\"}}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697890382360,\"lte\":1697891282360,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT `s1` as `3_s1`,count(1) as `3__ckCount`,toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1697891280000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1697890380000  ) AND (`3_s1` is not null AND `3_s1` != '' ) GROUP BY `2_@timestampDateTime`,`3_s1` ORDER BY `2_@timestampDateTime` asc,`s1` asc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_TERMS_AGG_ORDER_BY_ALPHABETICAL_ASC_BASE_ON_DATE_HISTOGRAM, query, Boolean.FALSE, expectedSqls);
    }
    /**
     * TermsAggOrderType.ALPHABETICAL desc 饼图
     */
    @Test
    public void testTermsAggOrderTypeMetricCustomAscOnly() {
        String query = "{\"aggs\":{\"2\":{\"terms\":{\"field\":\"s1\",\"size\":5,\"order\":{\"1\":\"desc\"}},\"aggs\":{\"1\":{\"sum\":{\"field\":\"i1\"}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1699446469750,\"lte\":1699447369750,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"300000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT SUM(`i1`) as `1_i1`,`s1` as `2_s1`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  toUnixTimestamp64Milli(`@timestampDateTime`) <= 1699447360000  AND  toUnixTimestamp64Milli(`@timestampDateTime`) >= 1699446460000  ) AND (`2_s1` is not null AND `2_s1` != '' ) GROUP BY `2_s1` ORDER BY `1_i1` desc LIMIT 5\"\n" +
                "            ]";
        doTest(TEST_DEMO_TERMS_AGG_ORDER_BY_ALPHABETICAL_ASC_BASE_ON_DATE_HISTOGRAM, query, Boolean.FALSE, expectedSqls);
    }
}
