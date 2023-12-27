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
package com.ly.ckibana.model.compute.aggregation;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.enums.AggBucketsName;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.strategy.aggs.Aggregation;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * agg的解析配置.
 */
@Data
@NoArgsConstructor
public class AggsParam {

    private String aggName;

    private int depth;

    private AggType aggType;

    private JSONObject aggSetting;

    private String field;

    private String fieldType;

    private String commonQuery;

    private boolean isKeyed;

    private Map<String, String> columns;

    private List<Aggregation> subAggs;
    
    /**
     * agg返回的聚合结果的key名称.
     */
    private AggBucketsName aggBucketsName;
    
    public AggsParam(String aggName, int depth, String commonQuery,
                     Map<String, String> columns, List<Aggregation> subAggs) {
        this.aggName = aggName;
        this.depth = depth;
        this.commonQuery = commonQuery;
        this.columns = columns;
        this.subAggs = subAggs;
    }
}
