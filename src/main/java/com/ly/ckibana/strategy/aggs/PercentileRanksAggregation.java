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
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.aggregation.AggsParam;
import com.ly.ckibana.model.compute.aggregation.bucket.PercentilesRankBucket;
import com.ly.ckibana.model.compute.aggregation.bucket.PercentilesRankItemBucket;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.strategy.aggs.converter.CountSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import com.ly.ckibana.util.SqlUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Component
@NoArgsConstructor
@Data
public class PercentileRanksAggregation extends Aggregation {

    private List<Object> values;

    private String order;

    private List<String> names;

    public PercentileRanksAggregation(AggsParam aggsParam) {
        super(aggsParam);
        values = aggsParam.getAggSetting().getJSONArray(Constants.VALUES);
        names = new ArrayList<>();
    }

    @Override
    public PercentileRanksAggregation generate(AggsParam aggsParam) {
        return new PercentileRanksAggregation(aggsParam);
    }

    @Override
    public List<SqlConverter> buildSelectSqlConvertors(Range timeRange) {
        String field = getField();
        List<SqlConverter> result = new ArrayList<>();
        values.forEach(each -> {
            CountSqlConverter countAggregation = new CountSqlConverter();
            countAggregation.setCondition(SqlUtils.getCompareSql(SqlUtils.escape(field), Constants.Symbol.LTE, each));
            String itemName = field + SqlConstants.QUERY_NAME_SEPARATOR + each;
            countAggregation.setName(itemName);
            names.add(itemName);
            result.add(countAggregation);
        });
        CountSqlConverter countAggregation = new CountSqlConverter();
        countAggregation.setCondition(StringUtils.EMPTY);
        countAggregation.setName(queryAggCountName());
        result.add(countAggregation);
        return result;
    }

    @Override
    public PercentilesRankBucket buildResultBucket(JSONObject obj) {
        long count = obj.getLongValue(queryAggCountName());
        PercentilesRankBucket result = new PercentilesRankBucket();
        List<PercentilesRankItemBucket> items = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            double value = count > 0 ? (obj.getLongValue(names.get(i)) * 100 / (double) count) : 0;
            items.add(new PercentilesRankItemBucket(values.get(i), value));
        }
        result.setItems(items);
        Object termValue = obj.get(queryFieldName());
        result.setKey(termValue);
        result.setDocCount(count);
        return result;
    }

    @Override
    public AggType getAggType() {
        return AggType.PERCENTILE_RANKS;
    }
}
