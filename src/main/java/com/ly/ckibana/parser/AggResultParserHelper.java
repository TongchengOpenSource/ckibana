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
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.aggregation.RangeAggItem;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.request.CkRequest;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.strategy.aggs.Aggregation;
import com.ly.ckibana.strategy.aggs.RangeAggregation;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.SqlUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 封装结果-辅助类。主要用于多次ck结果合并.
 *
 * @author zl11357
 * @since 2023/10/30 16:32
 */
@Service
public class AggResultParserHelper {
    public static final String KEY = "key";

    /**
     * 获取需要额外为子agg添加的条件。支持range agg和terms agg.
     * 父agg range为查询range item条件生成的比较条件
     * 父agg terms 为terms agg结果中的值，生成对应等于条件
     *
     * @param parentAggregation parentAggregation
     * @param parentBucket      parentBucket
     * @return Map
     */
    public Map<Object, String> buildAppendWhereForSubAgg(Aggregation parentAggregation, Object parentBucket) {
        Map<Object, String> conditionByKey = new HashMap<>();
        if (parentAggregation.getAggType().equals(AggType.TERMS)) {
            if (parentAggregation.isKeyed()) {
                JSONObject mapBuckets = JSONUtils.convert(parentBucket, JSONObject.class);
                mapBuckets.keySet().forEach(v -> conditionByKey.put(v, SqlUtils.getEqualsSql(parentAggregation.queryFieldName(), v, Boolean.TRUE)));
            } else {
                List<JSONObject> listBuckets = JSONUtils.convert(parentBucket, List.class);
                listBuckets.forEach(v ->
                        conditionByKey.put(v.get(KEY),
                                SqlUtils.getEqualsSql(ProxyUtils.getFieldSqlPart(parentAggregation.queryFieldName()),
                                        v.get(KEY), StringUtils.equals(SqlConstants.TYPE_STRING, parentAggregation.getFieldType()))));
            }
        } else if (AggType.RANGE.equals(parentAggregation.getAggType())) {
            for (RangeAggItem rangeAggItem : ((RangeAggregation) parentAggregation).getRanges()) {
                Range range = getRange((RangeAggregation) parentAggregation, rangeAggItem);
                conditionByKey.put(rangeAggItem.getKey(), range.toSql(Boolean.FALSE));
            }
        }
        return conditionByKey;
    }

    /**
     * 将RangeAggregation的一个item转换为Range.
     *
     * @param rangeAggregation rangeAggregation
     * @param rangeAggItem     rangeAggItem
     * @return Range
     */

    private Range getRange(RangeAggregation rangeAggregation, RangeAggItem rangeAggItem) {
        Range range = new Range(rangeAggregation.queryFieldName(), rangeAggregation.getFieldType());
        if (null != rangeAggItem.getFrom() && !rangeAggregation.isMatchAll(rangeAggItem.getFrom().toString())) {
            range.setLow(rangeAggItem.getFrom().toString());
        }
        if (null != rangeAggItem.getTo() && !rangeAggregation.isMatchAll(rangeAggItem.getTo().toString())) {
            range.setHigh(rangeAggItem.getTo().toString());
        }
        range.setLessThanEq(Boolean.FALSE);
        range.setMoreThanEq(Boolean.TRUE);
        return range;
    }

    public CkRequest getSubAggCkRequestByParentAgg(CkRequestContext ckRequestContext,
                                                   Aggregation parentAgg,
                                                   Aggregation subAgg,
                                                   String parentWhereSql) {
        CkRequest subAggCkRequest = subAgg.buildCkRequest(ckRequestContext);
        subAggCkRequest.appendWhere(parentWhereSql);
        if (CollectionUtils.isNotEmpty(parentAgg.buildGroupBySql())) {
            subAggCkRequest.appendSelect(SqlUtils.getColumnAsAliasString(parentAgg.getField(), parentAgg.queryFieldName()));
            subAggCkRequest.appendGroupBy(parentAgg.queryFieldName());
        }
        return subAggCkRequest;

    }

}
