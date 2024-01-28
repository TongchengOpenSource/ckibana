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
import com.ly.ckibana.model.enums.AggCategory;
import com.ly.ckibana.model.enums.AggType;
import org.junit.Test;

/**
 * msearch agg测试
 *
 * @author zl11357
 * @since 2023/10/19 14:43
 */
public class CommonAggTest extends CommonTest {
    public static final String NAME_AGGREGATION = "Aggregation";
    public static final String TEST_AGGS_FILTERS_AND_DATE_HISTOGRAM_AGG = getTestName(NAME_AGGREGATION, AggType.FILTERS.name(), AggType.DATE_HISTOGRAM.name());
    public static final String TEST_AGGS_DATE_HISTOGRAM_AGG = getTestName(NAME_AGGREGATION, AggType.DATE_HISTOGRAM.name());
    public static final String TEST_DEMO_TERMS_AGG = getTestName(NAME_AGGREGATION, AggType.TERMS.name());
    public static final String TEST_DEMO_MATH_AND_PERCENTILE_DATE_HISTOGRAM_AGG = getTestName(NAME_AGGREGATION, AggType.DATE_HISTOGRAM.name(), AggCategory.MATH.name(), AggType.PERCENTILE_RANKS.name(), AggType.PERCENTILES.name());
    public static final String TEST_DEMO_MATH_AND_PERCENTILE_AGG = getTestName(NAME_AGGREGATION, AggCategory.MATH.name(), AggType.PERCENTILE_RANKS.name(), AggType.PERCENTILES.name());
    public static final String TEST_DEMO_RANGE_NUMBER_AGG = getTestName(NAME_AGGREGATION, AggType.RANGE.name());
    public static final String TEST_DEMO_CARDINALITY_AGG = getTestName(NAME_AGGREGATION, AggType.CARDINALITY.name());

    /**
     * AggType.FILTERS AggType.DATE_HISTOGRAM
     */
    @Test
    public void testFiltersAndDateHistogramAgg() {
        String query = "{\"aggs\":{\"2\":{\"filters\":{\"filters\":{\"s1:\\\"s1value1\\\"\":{\"query_string\":{\"query\":\"s1:\\\"s1value1\\\"\",\"analyze_wildcard\":true,\"default_field\":\"*\"}},\"s2:\\\"s1value2\\\"\":{\"query_string\":{\"query\":\"s2:\\\"s1value2\\\"\",\"analyze_wildcard\":true,\"default_field\":\"*\"}}}},\"aggs\":{\"3\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697890656331,\"lte\":1697891556331,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `3_@timestampDateTime`,count(1) as `3__ckCount`,count(1) as `s1:\\\"s1value1\\\"__ckCount` FROM `table1_all` PREWHERE (  `@timestampDateTime` <= toDateTime64(1697891550000/1000.0)  AND  `@timestampDateTime` >= toDateTime64(1697890650000/1000.0)  ) AND ( (  `s1` like '%s1value1%' ) ) GROUP BY `3_@timestampDateTime` ORDER BY `3_@timestampDateTime` asc LIMIT 30000\",\n" +
                "                \"SELECT toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `3_@timestampDateTime`,count(1) as `3__ckCount`,count(1) as `s2:\\\"s1value2\\\"__ckCount` FROM `table1_all` PREWHERE (  `@timestampDateTime` <= toDateTime64(1697891550000/1000.0)  AND  `@timestampDateTime` >= toDateTime64(1697890650000/1000.0)  ) AND ( (  `s2` like '%s1value2%' ) ) GROUP BY `3_@timestampDateTime` ORDER BY `3_@timestampDateTime` asc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_AGGS_FILTERS_AND_DATE_HISTOGRAM_AGG, query, Boolean.FALSE, expectedSqls);
    }

    /**
     * AggType.DATE_HISTOGRAM
     */
    @Test
    public void testDateHistogramAgg() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"query_string\":{\"query\":\"s1:\\\"s1value\\\"\",\"analyze_wildcard\":true,\"default_field\":\"*\"}},{\"query_string\":{\"query\":\"s1:\\\"s1value\\\"\",\"analyze_wildcard\":true,\"default_field\":\"*\"}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697890734041,\"lte\":1697891634041,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE ( (  `s1` like '%s1value%' )  AND  (  `s1` like '%s1value%' ) ) AND ((  `@timestampDateTime` <= toDateTime64(1697891630000/1000.0)  AND  `@timestampDateTime` >= toDateTime64(1697890730000/1000.0)  )) GROUP BY `2_@timestampDateTime` ORDER BY `2_@timestampDateTime` asc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_AGGS_DATE_HISTOGRAM_AGG, query, Boolean.FALSE, expectedSqls);

    }

    /**
     * AggType.TERMS
     */
    @Test
    public void testTermsAgg() {
        String query = "{\"aggs\":{\"2\":{\"terms\":{\"field\":\"s1\",\"size\":10,\"order\":{\"_count\":\"desc\"}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697891206239,\"lte\":1697892106239,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT `s1` as `2_s1`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  `@timestampDateTime` <= toDateTime64(1697892100000/1000.0)  AND  `@timestampDateTime` >= toDateTime64(1697891200000/1000.0)  ) AND (`2_s1` is not null AND `2_s1` != '' ) GROUP BY `2_s1` ORDER BY `2__ckCount` desc LIMIT 10\"\n" +
                "            ]";
        doTest(TEST_DEMO_TERMS_AGG, query, Boolean.FALSE, expectedSqls);
    }

    /**
     * AggCategory.AVG
     * AggCategory.MIN
     * AggCategory.MAX
     * AggCategory.SUM
     * AggType.PERCENTILE_RANKS
     * AggType.PERCENTILES;
     * +AggType.DateHistogram
     * }
     */
    @Test
    public void testMathAndPercentAndDateHistogramAgg() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"3\":{\"sum\":{\"field\":\"i1\"}},\"4\":{\"min\":{\"field\":\"i2\"}},\"5\":{\"max\":{\"field\":\"i3\"}},\"6\":{\"avg\":{\"field\":\"i1\"}},\"7\":{\"percentiles\":{\"field\":\"i1\",\"percents\":[90,95,99],\"keyed\":false}},\"8\":{\"percentile_ranks\":{\"field\":\"i2\",\"values\":[100],\"keyed\":false}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697891258636,\"lte\":1697892158636,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT SUM(`i1`) as `3_i1`,MIN(`i2`) as `4_i2`,MAX(`i3`) as `5_i3`,avg(`i1`) as `6_i1`,quantile(0.9)(`i1`) as `i1_90`,quantile(0.95)(`i1`) as `i1_95`,quantile(0.99)(`i1`) as `i1_99`,count(1) as `7__ckCount`,countIf(`i2` <= 100) as `i2_100`,count(1) as `8__ckCount`,toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  `@timestampDateTime` <= toDateTime64(1697892150000/1000.0)  AND  `@timestampDateTime` >= toDateTime64(1697891250000/1000.0)  ) GROUP BY `2_@timestampDateTime` ORDER BY `2_@timestampDateTime` asc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_MATH_AND_PERCENTILE_DATE_HISTOGRAM_AGG, query, Boolean.FALSE, expectedSqls);
    }

    /**
     * AggCategory.AVG
     * AggCategory.MIN
     * AggCategory.MAX
     * AggCategory.SUM
     * AggType.PERCENTILE_RANKS+AggType.PERCENTILES;
     * +没有AggType.DateHistogram
     * }
     */
    @Test
    public void testMathAndPercentAgg() {
        String query = "{\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"@timestampDateTime\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"3\":{\"sum\":{\"field\":\"i1\"}},\"4\":{\"min\":{\"field\":\"i2\"}},\"5\":{\"max\":{\"field\":\"i3\"}},\"6\":{\"avg\":{\"field\":\"i1\"}},\"7\":{\"percentiles\":{\"field\":\"i1\",\"percents\":[90,95,99],\"keyed\":false}},\"8\":{\"percentile_ranks\":{\"field\":\"i2\",\"values\":[100],\"keyed\":false}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697891258636,\"lte\":1697892158636,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT SUM(`i1`) as `3_i1`,MIN(`i2`) as `4_i2`,MAX(`i3`) as `5_i3`,avg(`i1`) as `6_i1`,quantile(0.9)(`i1`) as `i1_90`,quantile(0.95)(`i1`) as `i1_95`,quantile(0.99)(`i1`) as `i1_99`,count(1) as `7__ckCount`,countIf(`i2` <= 100) as `i2_100`,count(1) as `8__ckCount`,toInt64((toUnixTimestamp64Milli(`@timestampDateTime`)) / 30000) as `2_@timestampDateTime`,count(1) as `2__ckCount` FROM `table1_all` PREWHERE (  `@timestampDateTime` <= toDateTime64(1697892150000/1000.0)  AND  `@timestampDateTime` >= toDateTime64(1697891250000/1000.0)  ) GROUP BY `2_@timestampDateTime` ORDER BY `2_@timestampDateTime` asc LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_MATH_AND_PERCENTILE_AGG, query, Boolean.FALSE, expectedSqls);
    }

    /**
     * 数值型range
     * AggType.RANGE
     */
    @Test
    public void testRangeNumber() {
        String query = "{\"aggs\":{\"2\":{\"range\":{\"field\":\"i1\",\"ranges\":[{\"from\":0,\"to\":1000},{\"from\":1000,\"to\":2000},{\"from\":3000}],\"keyed\":true}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697891642926,\"lte\":1697892542926,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = " [\n" +
                "                \"SELECT countIf(`i1` >= 0 AND `i1` < 1000) as `0-1000`,countIf(`i1` >= 1000 AND `i1` < 2000) as `1000-2000`,countIf(`i1` >= 3000) as `3000-*` FROM `table1_all` PREWHERE (  `@timestampDateTime` <= toDateTime64(1697892540000/1000.0)  AND  `@timestampDateTime` >= toDateTime64(1697891640000/1000.0)  ) AND (`i1` != 2147483647) LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_RANGE_NUMBER_AGG, query, Boolean.FALSE, expectedSqls);
    }

    /**
     * AggType.CARDINALITY
     */
    @Test
    public void testCardinality() {
        String query = "{\"aggs\":{\"2\":{\"range\":{\"field\":\"i1\",\"ranges\":[{\"from\":0,\"to\":100},{\"from\":100,\"to\":200},{\"from\":200,\"to\":300}],\"keyed\":true}}},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},\"docvalue_fields\":[{\"field\":\"@timestampDateTime\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_all\":{}},{\"match_all\":{}},{\"range\":{\"@timestampDateTime\":{\"gte\":1697891714119,\"lte\":1697892614119,\"format\":\"epoch_millis\"}}}],\"filter\":[],\"should\":[],\"must_not\":[]}},\"timeout\":\"120000ms\"}";
        String expectedSqls = "[\n" +
                "                \"SELECT countIf(`i1` >= 0 AND `i1` < 100) as `0-100`,countIf(`i1` >= 100 AND `i1` < 200) as `100-200`,countIf(`i1` >= 200 AND `i1` < 300) as `200-300` FROM `table1_all` PREWHERE (  `@timestampDateTime` <= toDateTime64(1697892610000/1000.0)  AND  `@timestampDateTime` >= toDateTime64(1697891710000/1000.0)  ) AND (`i1` != 2147483647) LIMIT 30000\"\n" +
                "            ]";
        doTest(TEST_DEMO_CARDINALITY_AGG, query, Boolean.FALSE, expectedSqls);
    }
}
