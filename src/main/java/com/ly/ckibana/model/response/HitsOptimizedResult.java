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

import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.enums.SortType;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * hits优化查询条件后的结果.
 * @author zl11357
 * @since 2023/10/10 15:10
 */
@Data
@AllArgsConstructor
public class HitsOptimizedResult {
    /**
     * 排序类型.
     */
    private SortType sortType;
    
    /**
     * 优化后的时间区间.
     */
    private Range optimizedTimeRange;

    /**
     * 是否命中缓存.
     */
    private boolean cache;
    
    /**
     * 基于优化策略计算得到的明细总数。无需二次查询明细总数.
     */
    private Long totalCount;
    
    /**
     * 是否可被优化.
     */
    private boolean isOptimized;

    private String countByMinutesQuerySql;

    public HitsOptimizedResult() {
    }

}
