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
import com.ly.ckibana.strategy.aggs.converter.FieldSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import com.ly.ckibana.model.compute.aggregation.AggsParam;
import com.ly.ckibana.model.compute.aggregation.bucket.MathBucket;
import com.ly.ckibana.model.enums.AggCategory;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.SqlUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Component
@NoArgsConstructor
@Data
public class MathCategoryAggregation extends Aggregation {

    public MathCategoryAggregation(AggsParam aggsParam) {
        super(aggsParam);
        this.setAggCategory(AggCategory.MATH);
    }

    /**
     * 这方法名实在没看懂是啥.
     *
     * @param mathKey mathKey
     * @return List
     */
    public List<SqlConverter> buildMathCategorySelectSql(String mathKey) {
        List<SqlConverter> result = new ArrayList<>();
        FieldSqlConverter fieldAggregation = new FieldSqlConverter();
        fieldAggregation.setCondition(SqlUtils.getFunctionString(mathKey, ProxyUtils.getFieldSqlPart(getField())));
        fieldAggregation.setName(queryFieldName());
        result.add(fieldAggregation);
        return result;
    }

    @Override
    public MathBucket buildResultBucket(JSONObject obj) {
        MathBucket result = new MathBucket();
        result.setKey(queryFieldName());
        result.setValue(obj.getDoubleValue(queryFieldName()));
        return result;
    }
}
