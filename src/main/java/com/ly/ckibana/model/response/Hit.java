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

import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 搜索结果.
 *
 * @author quzhihao
 */
@Data
public class Hit {

    @JsonProperty("_id")
    private String id;

    @JsonProperty("_index")
    private String index;

    @JsonProperty("_version")
    private long version = 1;

    @JsonProperty("_score")
    private int score;

    @JsonProperty("_source")
    private JSONObject source;

    @JsonProperty("_type")
    private String type = "info";

    private Map<String, Object> fields;

    private List<Object> sort;
}
