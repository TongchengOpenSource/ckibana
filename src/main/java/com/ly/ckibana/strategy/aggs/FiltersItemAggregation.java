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
package com.ly.ckibana.strategy.aggs;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.aggregation.AggsParam;
import com.ly.ckibana.model.compute.aggregation.bucket.FilterInnerBucket;
import com.ly.ckibana.model.enums.AggBucketsName;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.request.CkRequest;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.strategy.aggs.converter.CountSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * filters中的一个查询。含独立query条件.
 */
@EqualsAndHashCode(callSuper = true)
@Component
@NoArgsConstructor
@Data
public class FiltersItemAggregation extends Aggregation {

    private String filterQuerySql;

    public FiltersItemAggregation(AggsParam aggsParam, String filterName, String filterQuerySql) {
        super(AggType.FILTERS_ITEM, filterName, aggsParam.getCommonQuery(), aggsParam.isKeyed(),
                aggsParam.getDepth(), filterName, null, AggBucketsName.BUCKETS);
        setFilterQuerySql(filterQuerySql);
    }

    @Override
    public List<SqlConverter> buildSelectSqlConvertors(Range timeRange) {
        List<SqlConverter> result = new ArrayList<>();
        CountSqlConverter countAggregation = new CountSqlConverter();
        countAggregation.setCondition(StringUtils.EMPTY);
        countAggregation.setName(queryAggCountName());
        result.add(countAggregation);
        return result;
    }

    @Override
    public FilterInnerBucket buildResultBucket(JSONObject obj) {
        FilterInnerBucket result = new FilterInnerBucket();
        result.setKey(queryFieldName());
        result.setDocCount(obj.getLong(queryAggCountName()));
        return result;
    }

    @Override
    public void appendAggsConditions(CkRequest ckRequest, CkRequestContext ckRequestContext) {
        ckRequest.appendWhere(filterQuerySql);
        super.appendAggsConditions(ckRequest, ckRequestContext);
    }
}
