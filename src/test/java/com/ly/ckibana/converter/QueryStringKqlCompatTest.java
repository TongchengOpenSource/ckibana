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
 * KQL/Lucene 常用语法兼容测试：- 排除、* 和 ? 通配符.
 */
public class QueryStringKqlCompatTest extends CommonTest {

    /**
     * Lucene 查询串：- 排除、* 通配符、? 通配符、.keyword 通配符.
     */
    @Test
    public void testNegatedFieldAndWildcardInQueryString() {
        String query = buildQuery("{\"bool\":{\"must\":[{\"query_string\":{\"query\":"
                + "\"-s1:\\\"s1value\\\" AND s2:\\\"pre*\\\" AND -i1:1 AND -s3:(\\\"s3value1\\\",\\\"s3value2\\\") "
                + "AND s1.keyword:\\\"fix*\\\" AND s2:\\\"a?c\\\"\",\"analyze_wildcard\":true,\"default_field\":\"*\"}}],"
                + "\"filter\":[],\"should\":[],\"must_not\":[]}}");
        String expectedSqls = "[\"SELECT count(1) as _count FROM `table1_all` PREWHERE ( (  NOT  `s1` like '%s1value%' "
                + "AND  `s2` like 'pre%' AND NOT  `i1` = 1 AND NOT  (`s3` like '%s3value1%' OR `s3` like '%s3value2%') "
                + "AND  `s1` like 'fix%' AND  `s2` like 'a_c' ) )\"]";
        doTest("NEGATED_FIELD_AND_WILDCARD_QUERY_STRING", query, Boolean.FALSE, expectedSqls);
    }

    /**
     * KQL 字段匹配生成 match 查询.
     */
    @Test
    public void testKqlMatchQuery() {
        String query = buildQuery("{\"bool\":{\"must\":[],\"filter\":[{\"bool\":{\"should\":[{\"match\":{\"s1\":\"s1value\"}}],"
                + "\"minimum_should_match\":1}}],\"should\":[],\"must_not\":[]}}");
        String expectedSqls = "[\"SELECT count(1) as _count FROM `table1_all` PREWHERE (((`s1` like '%s1value%')))\"]";
        doTest("KQL_MATCH", query, Boolean.FALSE, expectedSqls);
    }

    /**
     * KQL 减号排除生成 must_not 单对象 + match 查询.
     */
    @Test
    public void testKqlNegatedMatchQuery() {
        String query = buildQuery("{\"bool\":{\"must\":[],\"filter\":[{\"bool\":{\"must_not\":{\"bool\":{\"should\":"
                + "[{\"match\":{\"s1\":\"s1value\"}}],\"minimum_should_match\":1}},\"must\":[],\"should\":[],\"filter\":[]}}],"
                + "\"should\":[],\"must_not\":[]}}");
        String expectedSqls = "[\"SELECT count(1) as _count FROM `table1_all` PREWHERE (( NOT ((`s1` like '%s1value%'))))\"]";
        doTest("KQL_NEGATED_MATCH", query, Boolean.FALSE, expectedSqls);
    }

    /**
     * KQL 通配符生成 query_string:{fields:[...], query:"abc*"}.
     */
    @Test
    public void testKqlWildcardQuery() {
        String query = buildQuery("{\"bool\":{\"must\":[],\"filter\":[{\"query_string\":{\"fields\":[\"s1\"],"
                + "\"query\":\"s1value*\"}}],\"should\":[],\"must_not\":[]}}");
        String expectedSqls = "[\"SELECT count(1) as _count FROM `table1_all` PREWHERE ( (  `s1` like 's1value%' ) )\"]";
        doTest("KQL_WILDCARD", query, Boolean.FALSE, expectedSqls);
    }

    private String buildQuery(String query) {
        return "{\"aggs\":{},\"size\":0,\"_source\":{\"excludes\":[]},\"stored_fields\":[\"*\"],\"script_fields\":{},"
                + "\"docvalue_fields\":[],\"query\":" + query + ",\"timeout\":\"120000ms\"}";
    }
}
