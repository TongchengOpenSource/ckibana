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
package com.ly.ckibana.model.enums;

/**
 * terms聚合支持的排序类型.
 * field:按特定字段值排序
 * MetricCount:按聚合统计值排序
 * MetricCustom:按子查询值排序。比如max(request_time)
 * peerAgg:按同级别聚合值排序
 */
public enum TermsAggOrderType {
    ALPHABETICAL, METRIC_COUNT, METRIC_CUSTOM, PEER_AGG
}
