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
package com.ly.ckibana;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.property.KibanaProperty;
import com.ly.ckibana.model.property.QueryProperty;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.parser.HitsResultParser;
import com.ly.ckibana.parser.MsearchParamParser;
import com.ly.ckibana.strategy.aggs.Aggregation;
import com.ly.ckibana.strategy.aggs.FiltersAggregation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ut 公用方法类
 *
 * @author zl11357
 * @since 2023/10/19 14:43
 */
@TestPropertySource(properties = {"spring.config.location=classpath:application-ut.yml", "logging.config=classpath:logback-spring-ut.xml"})
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Bootstrap.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Slf4j
public class CommonTest {
    //时间字段
    private static final String timeField = "@timestampDateTime";
    //table信息
    private static final String tableColumnsCacheJson = "{\"table1_all\":{\"s3\":\"String\",\"ip\":\"String\",\"i1\":\"Float32\",\"i2\":\"Float32\",\"i3\":\"Float32\",\"ip2\":\"String\",\"ip1\":\"String\",\"ip3\":\"String\",\"@timestampDateTime\":\"DateTime64(3)\",\"ck_assembly_extension\":\"String\",\"@timestamp\":\"Int64\",\"ipv4\":\"IPv4\",\"ipv6\":\"IPv6\",\"ipv4_string\":\"String\",\"ipv6_string\":\"String\",\"s1\":\"String\",\"s2\":\"String\"}}";
    //indexpattern信息
    private static final String indexPatternJson = "{\"uiIndex\":\"table1_all\",\"cluster\":\"\",\"index\":\"table1_all\",\"database\":\"testdb\",\"timeField\":\"@timestampDateTime\",\"needSample\":false}";
    //kibana代理配置信息
    private static final String kibanaPropertyJson = "{\"defaultShard\":2,\"majorVersion\":6,\"proxy\":{\"ck\":{\"defaultCkDatabase\":\"testdb\",\"pass\":\"fc/3EtAe\",\"url\":\"10.177.43.183:6321\",\"user\":\"limited\"},\"es\":{\"headers\":{\"stoken\":\"b7842e1285e1d277e1730c41\"},\"host\":\"10.100.218.58:30691,10.100.218.60:30691,10.100.218.61:30691,10.100.218.62:30691,10.100.218.63:30691,10.100.218.64:30691\"},\"maxTimeRange\":86400000,\"roundAbleMinPeriod\":120000,\"whiteIndexList\":[\"table1_all\"]},\"query\":{\"sampleCountMaxThreshold\":1500000,\"useCache\":false,\"maxResultRow\":30000},\"threadPool\":{\"msearchProperty\":{\"coreSize\":100,\"queueSize\":10000}},\"yaml\":{\"name\":\"Yaml:1569371800\"}}";
    @Resource
    private MsearchParamParser msearchParamParser;
    @Resource
    private ProxyConfigLoader proxyConfigLoader;
    @Resource
    private HitsResultParser hitsResultParser;

    /**
     * 获取测试用例别名
     *
     * @param name
     * @return
     */
    public static String getTestName(String... name) {
        return Arrays.stream(name).collect(Collectors.joining("_"));
    }

    /**
     * demo test
     */
    @Test
    public void testCommon() {
        Assert.assertTrue("testCommon", Boolean.TRUE);
    }

    /**
     * 基于查询参数，转换得到sql列表，并于期望值比对。比对一致则通过，不一致则失败
     *
     * @param searchQueryJson 参数
     * @param needQueryHits   是否需要额外解析hits明细查询sql
     * @param expectedSqlJson 期望的sql列表
     */
    public void doTest(String testName, String searchQueryJson, Boolean needQueryHits, String expectedSqlJson) {
        try {
            proxyConfigLoader.setKibanaProperty(JSONObject.parseObject(kibanaPropertyJson, KibanaProperty.class));
            List<String> expectedSqlList = JSONObject.parseObject(expectedSqlJson, List.class);
            List<String> resultSqlList = convert2SqlList(searchQueryJson, needQueryHits);
            assertResult(testName, resultSqlList, expectedSqlList);
        } catch (Exception e) {
            log.error("doTest", e);
            Assert.assertTrue(Boolean.FALSE);
        }

    }

    /**
     * 基于查询参数，转换得到sql列表
     *
     * @param searchQueryJson
     * @param needQueryHits
     * @return
     * @throws Exception
     */
    private List<String> convert2SqlList(String searchQueryJson, Boolean needQueryHits) throws Exception {
        CkRequestContext ckRequestContext = testParseCkRequestContext(searchQueryJson, null);
        List<String> sqlList = new ArrayList<>();
        collectAggSqls(ckRequestContext, sqlList);
        if (ckRequestContext.getAggs().isEmpty() && ckRequestContext.getSize() == 0) {
            sqlList.add(hitsResultParser.getTotalCountQuerySql(ckRequestContext));
        }
        if (needQueryHits) {
            sqlList.add(hitsResultParser.buildHitRequest(ckRequestContext).buildToStr());
        }
        return sqlList;
    }

    /**
     * 基于查询上下文参数，解析得到agg sql列表
     *
     * @param ckRequestContext 查询上下文参数
     * @param sqlList          sql结果集
     */
    private void collectAggSqls(CkRequestContext ckRequestContext, List<String> sqlList) {
        List<Aggregation> filtersAggs = ckRequestContext.getAggs().stream().filter(each -> AggType.FILTERS.equals(each.getAggType())).toList();
        if (!filtersAggs.isEmpty()) {
            FiltersAggregation filtersAgg = (FiltersAggregation) filtersAggs.get(0);
            List<Aggregation> subAggs = filtersAgg.getSubAggs();
            for (int i = 0; i < filtersAgg.getFiltersItems().size(); i++) {
                Aggregation firstAgg = filtersAgg.getFiltersItems().get(i);
                firstAgg.setSubAggs(subAggs);
                sqlList.add(firstAgg.buildCkRequest(ckRequestContext).buildToStr());
            }
        } else {
            if (CollectionUtils.isNotEmpty(ckRequestContext.getAggs())) {
                for (Aggregation aggregation : ckRequestContext.getAggs()) {
                    sqlList.add(aggregation.buildCkRequest(ckRequestContext).buildToStr());
                }
            }
        }
    }

    /**
     * 将查询参数转换为查询上下文实体CkRequestContext，并去除用户类型动态参数（用户ip,查询开始时间）
     *
     * @param searchQueryJson
     * @param expectedCkRequestContextJson
     * @return
     * @throws Exception
     */
    private CkRequestContext testParseCkRequestContext(String searchQueryJson, String expectedCkRequestContextJson) throws Exception {
        CkRequestContext ckRequestContext = parseRequest(searchQueryJson);
        if (StringUtils.isNotBlank(expectedCkRequestContextJson)) {
            CkRequestContext expectedCkRequestContext = JSONObject.parseObject(expectedCkRequestContextJson, CkRequestContext.class);
            ckRequestContext.setBeginTime(expectedCkRequestContext.getBeginTime());
            ckRequestContext.setClientIp(expectedCkRequestContext.getClientIp());
            Assert.assertFalse("testParseCkRequestContext 失败", !JSONObject.toJSONString(ckRequestContext).equals(expectedCkRequestContextJson));
        }
        return ckRequestContext;
    }

    /**
     * 将查询参数转换为查询上下文实体CkRequestContext
     *
     * @param searchQueryJson
     * @return
     * @throws Exception
     */
    public CkRequestContext parseRequest(String searchQueryJson) throws Exception {
        BalancedClickhouseDataSource clickhouseDataSource = null;
        Map<String, Map<String, String>> tableColumnsCache = JSONObject.parseObject(tableColumnsCacheJson, Map.class);
        JSONObject searchQuery = JSONObject.parseObject(searchQueryJson);
        IndexPattern indexPattern = JSONObject.parseObject(indexPatternJson, IndexPattern.class);
        CkRequestContext ckRequestContext = new CkRequestContext("testIp", indexPattern,proxyConfigLoader.getKibanaProperty().getQuery().getMaxResultRow());
        QueryProperty queryProperty = proxyConfigLoader.getKibanaProperty().getQuery();
        CkRequestContext.SampleParam sampleParam = new CkRequestContext.SampleParam(Constants.USE_SAMPLE_COUNT_THREASHOLD, queryProperty.getSampleCountMaxThreshold());
        ckRequestContext.setSampleParam(sampleParam);
        msearchParamParser.parseRequestBySearchQuery(tableColumnsCache, searchQuery
                , timeField, indexPattern, ckRequestContext);

        return ckRequestContext;
    }

    /**
     * 判断结果是否成功,并记录日志
     *
     * @param testName
     * @param resultSqlList
     * @param expectedSqlList
     */
    private void assertResult(String testName, List<String> resultSqlList, List<String> expectedSqlList) {
        boolean isPassed = expectedSqlList.toString().equals(resultSqlList.toString());
        if (isPassed) {
            log.info(String.format("%s 测试通过", testName));
            log.info(String.format("%s 测试通过 resultSqlList = %s", testName, resultSqlList));
        } else {
            log.info(String.format("%s 测试失败, resultSqlList=%s\nexpectedSqlList=%s", testName, resultSqlList, expectedSqlList));
        }
        Assert.assertTrue(testName, isPassed);
    }


}
