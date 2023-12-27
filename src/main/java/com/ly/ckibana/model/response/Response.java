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
package com.ly.ckibana.model.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 响应结果.
 *
 * @author quzhihao
 */
@Data
public class Response {

    @JsonProperty("timed_out")
    private boolean timedOut;

    @JsonProperty("_shards")
    private Shards shards = new Shards();

    private Hits hits = new Hits();

    private int took;

    private boolean cache;

    /**
     * 聚合名-{buckets信息}.
     */
    private Map<String, Map<String, Object>> aggregations;

    private Integer status;

    private Object error;

    private List<String> sqls = new ArrayList<>();
}
