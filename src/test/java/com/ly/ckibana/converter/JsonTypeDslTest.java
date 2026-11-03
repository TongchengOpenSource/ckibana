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

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.CommonTest;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.exception.UnKnownFieldException;
import com.ly.ckibana.model.property.KibanaItemProperty;
import com.ly.ckibana.model.property.KibanaProperty;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.parser.HitsResultParser;
import com.ly.ckibana.parser.MsearchParamParser;
import com.ly.ckibana.strategy.aggs.Aggregation;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ClickHouse JSON/Object('json')子字段查询、排序与聚合的回归测试.
 */
public class JsonTypeDslTest extends CommonTest {

    private static final String JSON_TABLE = "github_json";

    private static final String TIME_FIELD = "@timestampDateTime";

    @Resource
    private MsearchParamParser msearchParamParser;

    @Resource
    private HitsResultParser hitsResultParser;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Test
    public void testJsonSubFieldQuery() throws Exception {
        CkRequestContext ckRequestContext = parseJsonRequest("{"
                + "\"size\":10,"
                + "\"sort\":[{\"event.n\":{\"order\":\"desc\"}}],"
                + "\"docvalue_fields\":[\"event.type\"],"
                + "\"query\":{\"bool\":{\"must\":["
                + "{\"term\":{\"event.type\":\"PushEvent\"}},"
                + "{\"range\":{\"event.n\":{\"gte\":5,\"lte\":10}}},"
                + "{\"exists\":{\"field\":\"event.actor.login\"}}]}},"
                + "\"aggs\":{\"2\":{\"terms\":{\"field\":\"event.type\",\"size\":5}}}}");

        String query = ckRequestContext.getQuery();
        Assert.assertTrue(query, query.contains("(`event.type` = 'PushEvent')"));
        Assert.assertTrue(query, query.contains("`event.n` <= 10"));
        Assert.assertTrue(query, query.contains("`event.n` >= 5"));
        Assert.assertTrue(query, query.contains("isNotNull(`event.actor.login`)"));
        Assert.assertEquals("`event.n` DESC", ckRequestContext.getSort());
        Assert.assertEquals("event.type", ckRequestContext.getDocValues().get(0).getCkFieldName());

        String hitsSql = hitsResultParser.buildHitRequest(ckRequestContext).buildToStr();
        Assert.assertTrue(hitsSql, hitsSql.contains("ORDER BY `event.n` DESC"));

        Assert.assertEquals(1, ckRequestContext.getAggs().size());
        Aggregation aggregation = ckRequestContext.getAggs().get(0);
        String aggSql = aggregation.buildCkRequest(ckRequestContext).buildToStr();
        Assert.assertTrue(aggSql, aggSql.contains("`event.type` as `2_event.type`"));
        Assert.assertTrue(aggSql, aggSql.contains("GROUP BY"));
    }

    @Test(expected = UnKnownFieldException.class)
    public void testJsonParentFieldNotQueryable() throws Exception {
        parseJsonRequest("{\"size\":10,\"query\":{\"bool\":{\"must\":["
                + "{\"term\":{\"event\":\"PushEvent\"}}]}}}");
    }

    private CkRequestContext parseJsonRequest(String searchQueryJson) throws Exception {
        initKibanaProperty();
        Map<String, String> columns = new LinkedHashMap<>();
        columns.put(TIME_FIELD, "DateTime64(3)");
        columns.put("event", "Object('json')");
        columns.put("event.actor.login", "String");
        columns.put("event.n", "Int64");
        columns.put("event.type", "String");
        Map<String, Map<String, String>> tableColumnsCache = new HashMap<>();
        tableColumnsCache.put(JSON_TABLE, columns);
        IndexPattern indexPattern = new IndexPattern();
        indexPattern.setUiIndex(JSON_TABLE);
        indexPattern.setIndex(JSON_TABLE);
        indexPattern.setDatabase("testdb");
        CkRequestContext ckRequestContext = new CkRequestContext("127.0.0.1", indexPattern, 30000);
        msearchParamParser.parseRequestBySearchQuery(tableColumnsCache, JSONObject.parseObject(searchQueryJson),
                TIME_FIELD, indexPattern, ckRequestContext);
        return ckRequestContext;
    }

    private void initKibanaProperty() {
        KibanaProperty kibanaProperty = new KibanaProperty();
        KibanaItemProperty proxy = new KibanaItemProperty();
        proxy.setRoundAbleMinPeriod(120000);
        proxy.setMaxTimeRange(86400000L);
        kibanaProperty.setProxy(proxy);
        proxyConfigLoader.setKibanaProperty(kibanaProperty);
    }
}
