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
package com.ly.ckibana.parser;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.strategy.aggs.Aggregation;
import com.ly.ckibana.strategy.aggs.AvgAggregation;
import com.ly.ckibana.strategy.aggs.MaxAggregation;
import com.ly.ckibana.util.JSONUtils;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ParamParserTest {
    @Test
    public void tests() {
        ParamParser paramParser = new ParamParser();
        paramParser.setAggStrategyMap(Map.of(
                AggType.MAX, new MaxAggregation(),
                AggType.AVG, new AvgAggregation()
        ));
        Map<String, Map<String, Map<String, Map<String, ?>>>> req = Map.of(
                "aggs", Map.of(
                        "by_user", Map.of(
                                "max", Map.of("field", "a"),
                                "avg", Map.of("field", "b"),
                                "aggs", Map.of(
                                        "by_type", Map.of(
                                                "max", Map.of("field", "c")
                                        ))
                        )
                ));

        JSONObject query = JSONUtils.deserialize(JSONUtils.serialize(req), JSONObject.class);
        CkRequestContext ckRequestContext = new CkRequestContext();
        ckRequestContext.setColumns(Map.of(
                "a", "String",
                "b", "String",
                "c", "String"
        ));
        List<Aggregation> aggregations = paramParser.parseAggs(0, ckRequestContext, query);
        assert aggregations.size() == 2;
        assert aggregations.get(0).getSubAggs().size() == 1;
        assert aggregations.get(1).getSubAggs().size() == 1;
    }
}
