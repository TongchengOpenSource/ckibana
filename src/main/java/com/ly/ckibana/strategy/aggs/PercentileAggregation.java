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
import com.ly.ckibana.model.compute.aggregation.bucket.PercentilesBucket;
import com.ly.ckibana.model.compute.aggregation.bucket.PercentilesItemBucket;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.strategy.aggs.converter.CountSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.FieldSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import com.ly.ckibana.util.ProxyUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
@Component
@NoArgsConstructor
@Data
public class PercentileAggregation extends Aggregation {

    private List<Integer> percents;

    private List<String> names;

    public PercentileAggregation(AggsParam aggsParam) {
        super(aggsParam);
        percents = aggsParam.getAggSetting().getJSONArray(Constants.PERCENTS).stream()
                .map(each -> Integer.parseInt(each.toString())).collect(Collectors.toList());
        names = new ArrayList<>();
    }

    @Override
    public PercentileAggregation generate(AggsParam aggsParam) {
        return new PercentileAggregation(aggsParam);
    }

    @Override
    public List<SqlConverter> buildSelectSqlConvertors(Range timeRange) {
        String field = getField();
        List<SqlConverter> result = new ArrayList<>();
        percents.forEach(each -> {
            String itemName = field + SqlConstants.QUERY_NAME_SEPARATOR + each;
            FieldSqlConverter fieldAggregation = new FieldSqlConverter();
            fieldAggregation.setCondition(String.format("quantile(%s)(%s)", each * 1.00 / 100, ProxyUtils.getFieldSqlPart(field)));
            fieldAggregation.setName(itemName);
            result.add(fieldAggregation);
            names.add(itemName);
        });
        CountSqlConverter countAggregation = new CountSqlConverter();
        countAggregation.setCondition("");
        countAggregation.setName(queryAggCountName());
        result.add(countAggregation);
        return result;
    }

    @Override
    public PercentilesBucket buildResultBucket(JSONObject obj) {
        Long count = obj.getLongValue(queryAggCountName());
        PercentilesBucket result = new PercentilesBucket();
        List<PercentilesItemBucket> items = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            String value = names.get(i);
            PercentilesItemBucket innerBucket = new PercentilesItemBucket();
            //最后一位为实际value
            innerBucket.setKey(percents.get(i));
            innerBucket.setValue(obj.get(value));
            items.add(innerBucket);
        }
        result.setItems(items);
        Object termValue = obj.get(queryFieldName());
        result.setKey(termValue);
        result.setDocCount(count);
        return result;
    }

    @Override
    public AggType getAggType() {
        return AggType.PERCENTILES;
    }
}
