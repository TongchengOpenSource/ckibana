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
package com.ly.ckibana.model.compute.aggregation.bucket;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

/**
 * Bucket.
 */
@Data
public class Bucket {
    
    /**
     * bucket key.
     */
    private Object key;
    
    /**
     * 文档数.
     */
    @JsonProperty("doc_count")
    private long docCount;
    
    /**
     * 计算用的内存临时数据.
     */
    private ComputeData computeData = new ComputeData();
    
    /**
     * 设置子聚合的计算数据.
     * @param childAggData 子聚合的计算数据
     */
    public void setComputeSubAggData(Map childAggData) {
        this.getComputeData().setSubAggData(childAggData);
    }

    @Data
    public static class ComputeData {
        private Map subAggData;

        private Map<String, BucketsResult> subBucketMap;

        private Map peerAggData;

        private Map<String, BucketsResult> peerBucketMap;
    }
    
}
