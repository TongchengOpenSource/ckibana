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

import com.google.common.collect.Lists;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.strategy.aggs.converter.CountSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.aggregation.AggsParam;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.SqlUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Component
@NoArgsConstructor
@Data
public class CardinalityAggregation extends MathCategoryAggregation {

    public CardinalityAggregation(AggsParam aggsParam) {
        super(aggsParam);
    }

    @Override
    public CardinalityAggregation generate(AggsParam aggsParam) {
        return new CardinalityAggregation(aggsParam);
    }

    @Override
    public List<SqlConverter> buildSelectSqlConvertors(Range timeRange) {
        CountSqlConverter countAggregation = new CountSqlConverter();
        countAggregation.setCondition(StringUtils.EMPTY);
        countAggregation.setName(queryFieldName());
        String condition = SqlUtils.getFunctionString(SqlConstants.DISTINCT, ProxyUtils.getFieldSqlPart(getField()));
        countAggregation.setFunctionCondition(condition);
        return Lists.newArrayList(countAggregation);
    }

    @Override
    public AggType getAggType() {
        return AggType.CARDINALITY;
    }
}
