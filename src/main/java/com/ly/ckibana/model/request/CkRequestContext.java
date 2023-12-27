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
package com.ly.ckibana.model.request;

import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.response.DocValue;
import com.ly.ckibana.strategy.aggs.Aggregation;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class CkRequestContext {

    private IndexPattern indexPattern;

    private String tableName;

    private Map<String, String> columns = new HashMap<>();

    private int size;

    private String sort;

    /**
     * response hits使用 使用原始字段名.
     */
    private List<DocValue> docValues;

    private List<SortedField> sortingFields;

    private String querySqlWithoutTimeRange;

    private String query;

    /**
     * 聚合参数.
     */
    private List<Aggregation> aggs;

    /**
     * 采样参数.
     */
    private SampleParam sampleParam;
    /**
     * 最大结果条数。若大于或等于此阈值,抛出异常,否则可能引发oom.
     */
    private int maxResultRow;

    /**
     * 时间范围参数.
     */
    private Range timeRange;

    /**
     * 记录耗时使用.
     */
    private long beginTime;

    private String database;

    private String clientIp;

    public CkRequestContext() {
    }

    public CkRequestContext(String clientIp, IndexPattern indexPattern, int maxResultRow) {
        this.clientIp = clientIp;
        this.indexPattern = indexPattern;
        this.maxResultRow = maxResultRow;
        database = indexPattern.getDatabase();
        beginTime = System.currentTimeMillis();
    }

    @Data
    @AllArgsConstructor
    public static class SampleParam {

        /**
         * 优化策略，一个msearch查询第一个的count,确定本次查询的采样参数.
         */
        private long sampleTotalCount;

        /**
         * sampleTotalCount数据量超过这个阈值，才会使用采样查询.
         */
        private int sampleCountMaxThreshold;

    }
}
