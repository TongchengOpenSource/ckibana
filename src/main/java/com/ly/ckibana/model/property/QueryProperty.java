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
package com.ly.ckibana.model.property;

import lombok.Data;

import java.util.List;

/**
 * 查询配置.
 *
 * @author quzhihao
 */
@Data
public class QueryProperty {

    /**
     * 需要采样的索引.
     */
    private List<String> sampleIndexPatterns;

    /**
     * 开启采样的阈值. 数据量超过这个阈值，才会开启采样
     */
    private int sampleCountMaxThreshold;

    /**
     * 开启缓存.
     */
    private boolean useCache;

    /**
     * 最大结果条数。若大于或等于此阈值,抛出异常,否则可能引发oom.
     */
    private int maxResultRow = 30000;
}
