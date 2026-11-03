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
package com.ly.ckibana.utils;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.enums.SortType;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.request.SortedField;
import com.ly.ckibana.model.response.DocValue;
import com.ly.ckibana.model.response.Hit;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.parser.HitsResultParser;
import com.ly.ckibana.parser.ParamParser;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.util.ProxyUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ClickHouse JSON/Object('json')类型支持的单测.
 */
public class JsonTypeSupportTest {

    @Test
    public void testIsJsonType() {
        Assert.assertTrue(ProxyUtils.isJsonType("JSON"));
        Assert.assertTrue(ProxyUtils.isJsonType("JSON(max_dynamic_paths=100)"));
        Assert.assertTrue(ProxyUtils.isJsonType("Object('json')"));
        Assert.assertTrue(ProxyUtils.isJsonType("Nullable(Object('json'))"));
        Assert.assertFalse(ProxyUtils.isJsonType("String"));
        Assert.assertFalse(ProxyUtils.isJsonType("Array(String)"));
        Assert.assertFalse(ProxyUtils.isJsonType("Tuple(id Int8, name String)"));
    }

    @Test
    public void testGetFieldSqlPartWithJsonSubColumn() {
        Assert.assertEquals("`s1`", ProxyUtils.getFieldSqlPart("s1"));
        Assert.assertEquals("`event.type`", ProxyUtils.getFieldSqlPart("event.type"));
        Assert.assertEquals("`event.actor.login`", ProxyUtils.getFieldSqlPart("event.actor.login"));
        Assert.assertEquals("JSONExtractString(event,'type')",
                ProxyUtils.getFieldSqlPart("JSONExtractString(event,'type')"));
    }

    @Test
    public void testMergeJsonSubColumns() {
        Map<String, String> baseColumns = new LinkedHashMap<>();
        baseColumns.put("id", "UInt32");
        baseColumns.put("event", "Object('json')");
        baseColumns.put("name", "String");

        Map<String, String> extendedColumns = new LinkedHashMap<>();
        extendedColumns.put("id", "UInt32");
        extendedColumns.put("event", "Tuple(actor Tuple(id Int8, login String), size0 UInt64, tags Array(String), type String)");
        extendedColumns.put("event.actor", "Tuple(id Int8, login String)");
        extendedColumns.put("event.actor.id", "Int8");
        extendedColumns.put("event.actor.login", "String");
        extendedColumns.put("event.objs", "Nested(a Int8)");
        extendedColumns.put("event.objs.a", "Array(Int8)");
        extendedColumns.put("event.objs.size0", "UInt64");
        extendedColumns.put("event.empty", "Tuple()");
        extendedColumns.put("event.size0", "UInt64");
        extendedColumns.put("event.tags", "Array(String)");
        extendedColumns.put("event.tags.size0", "UInt64");
        extendedColumns.put("event.type", "String");
        extendedColumns.put("name", "String");

        Map<String, String> result = ProxyUtils.mergeJsonSubColumns(baseColumns, extendedColumns);

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("id", "UInt32");
        expected.put("event", "Object('json')");
        expected.put("name", "String");
        expected.put("event.actor.id", "Int8");
        expected.put("event.actor.login", "String");
        expected.put("event.objs.a", "Array(Int8)");
        expected.put("event.size0", "UInt64");
        expected.put("event.tags", "Array(String)");
        expected.put("event.type", "String");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testBuildColumnsSql() {
        Assert.assertEquals("desc `github_json`", CkService.buildColumnsSql("github_json", false));
        Assert.assertEquals("desc `github_json` FORMAT TabSeparatedWithNamesAndTypes"
                        + " SETTINGS describe_include_subcolumns = 1, describe_extend_object_types = 1",
                CkService.buildColumnsSql("github_json", true));
    }

    @Test
    public void testJsonFieldsExposedInIndexPattern() throws Exception {
        ParamParser paramParser = new ParamParser();
        CkService ckService = Mockito.mock(CkService.class);
        ReflectionTestUtils.setField(paramParser, "ckService", ckService);

        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("@timestampDateTime", "DateTime64(3)");
        columns.put("event", "Object('json')");
        columns.put("event.actor.login", "String");
        columns.put("event.n", "Int64");
        columns.put("event.type", "String");
        Mockito.when(ckService.queryColumns(Mockito.any(), Mockito.eq("github_json"))).thenReturn(columns);

        ProxyConfig proxyConfig = Mockito.mock(ProxyConfig.class);
        Mockito.when(proxyConfig.getCkDatasource()).thenReturn(Mockito.mock(BalancedClickhouseDataSource.class));
        RequestContext requestContext = new RequestContext();
        requestContext.setProxyConfig(proxyConfig);
        IndexPattern indexPattern = new IndexPattern();
        indexPattern.setIndex("github_json");

        Map<String, JSONObject> fields = paramParser.queryIndexPatternFields(requestContext, indexPattern, false);

        Assert.assertTrue(fields.containsKey("event.type"));
        Assert.assertTrue(fields.containsKey("event.type.keyword"));
        Assert.assertTrue(fields.containsKey("event.n"));
        Assert.assertEquals("long", fields.get("event.n").keySet().iterator().next());
        Assert.assertFalse(fields.containsKey("event"));
    }

    @Test
    public void testJsonHitParsedToNestedSource() throws Exception {
        HitsResultParser hitsResultParser = new HitsResultParser();
        CkService ckService = Mockito.mock(CkService.class);
        ReflectionTestUtils.setField(hitsResultParser, "ckService", ckService);

        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("id", "UInt32");
        columns.put("@timestampDateTime", "DateTime64(3)");
        columns.put("event", "Object('json')");
        columns.put("event.n", "Int8");
        columns.put("event.type", "String");
        IndexPattern indexPattern = new IndexPattern();
        indexPattern.setIndex("github_json");
        indexPattern.setTimeField("@timestampDateTime");
        CkRequestContext ckRequestContext = new CkRequestContext("127.0.0.1", indexPattern, 30000);
        ckRequestContext.setTableName("github_json");
        ckRequestContext.setColumns(columns);
        ckRequestContext.setSize(1);
        ckRequestContext.setSort("`event.n` DESC");
        ckRequestContext.setQuery("");
        DocValue docValue = new DocValue();
        docValue.setFieldName("event.type");
        docValue.setCkFieldName("event.type");
        ckRequestContext.setDocValues(List.of(docValue));
        ckRequestContext.setSortingFields(List.of(new SortedField("event.n", "event.n", SortType.DESC)));

        JSONObject row = JSONObject.parseObject("{\"id\":1,\"@timestampDateTime\":\"2024-05-13 10:00:00\","
                + "\"event\":\"{\\\"type\\\":\\\"PushEvent\\\",\\\"n\\\":7}\"}");
        Mockito.when(ckService.queryDataWithCacheAndStatus(Mockito.any(), Mockito.anyString()))
                .thenReturn(Pair.of(List.of(row), false));

        Response response = new Response();
        hitsResultParser.queryHits(ckRequestContext, response);

        Hit hit = response.getHits().getHits().get(0);
        Assert.assertTrue(hit.getSource().get("event") instanceof JSONObject);
        Assert.assertEquals("PushEvent", ((JSONObject) hit.getSource().get("event")).getString("type"));
        Assert.assertEquals(List.of("PushEvent"), hit.getFields().get("event.type"));
        Assert.assertEquals(7, ((Number) hit.getSort().get(0)).intValue());

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(ckService).queryDataWithCacheAndStatus(Mockito.any(), sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        Assert.assertTrue(sql, sql.contains("toJSONString(`event`) as `event`"));
        Assert.assertFalse(sql, sql.contains("`event.type`"));
        Assert.assertTrue(sql, sql.contains("FORMAT TabSeparatedWithNamesAndTypes"
                + " SETTINGS output_format_json_named_tuples_as_objects = 1"));
    }
}
