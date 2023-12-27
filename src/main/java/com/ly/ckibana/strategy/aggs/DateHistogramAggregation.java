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
import com.google.common.collect.Lists;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.aggregation.AggsParam;
import com.ly.ckibana.model.compute.aggregation.Interval;
import com.ly.ckibana.model.compute.aggregation.bucket.DateHistogramBucket;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.strategy.aggs.converter.CountSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.FieldSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import com.ly.ckibana.util.DateUtils;
import com.ly.ckibana.util.ProxyUtils;
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
public class DateHistogramAggregation extends Aggregation {

    private long interval;

    public DateHistogramAggregation(AggsParam aggsParam) {
        super(aggsParam);
        String intervalKey = aggsParam.getAggSetting().containsKey(Constants.INTERVAL)
                ? Constants.INTERVAL
                : (aggsParam.getAggSetting().containsKey(Constants.FIXED_INTERVAL) ? Constants.FIXED_INTERVAL : Constants.CALENDAR_INTERVAL);
        Interval intervalObj = ProxyUtils.parseInterval(aggsParam.getAggSetting().getString(intervalKey));
        this.interval = intervalObj.getTimeUnit() * intervalObj.getValue();
    }

    @Override
    public DateHistogramAggregation generate(AggsParam aggsParam) {
        return new DateHistogramAggregation(aggsParam);
    }

    @Override
    public List<SqlConverter> buildSelectSqlConvertors(Range timeRange) {
        List<SqlConverter> result = new ArrayList<>();
        FieldSqlConverter fieldAggregation = new FieldSqlConverter();
        fieldAggregation.setCondition(
                SqlUtils.getFunctionString(SqlConstants.TO_INT64, "(%s) / %d",
                        ProxyUtils.generateTimeFieldSqlWithFormatUnixTimestamp64Milli(getField(), timeRange.getCkFieldType()),
                        getInterval())
        );
        fieldAggregation.setName(queryFieldName());
        result.add(fieldAggregation);
        CountSqlConverter countAggregation = new CountSqlConverter();
        countAggregation.setCondition(StringUtils.EMPTY);
        countAggregation.setName(queryAggCountName());
        result.add(countAggregation);
        return result;
    }

    @Override
    public List<String> buildOrdersBySql() {
        List<String> result = new ArrayList<>();
        result.add(SqlUtils.getOrderString(SqlUtils.escape(queryFieldName()), false));
        return result;
    }

    @Override
    public DateHistogramBucket buildResultBucket(JSONObject obj) {
        DateHistogramBucket result = new DateHistogramBucket();
        long key = getInterval() * obj.getLongValue(queryFieldName());
        long docCount = obj.getLongValue(queryAggCountName());
        result.setKey(key);
        result.setKeyAsString(DateUtils.getGMTOffsetEightHourDateStr(key));
        result.setDocCount(docCount);
        return result;
    }

    @Override
    public List<String> buildGroupBySql() {
        return Lists.newArrayList(SqlUtils.escape(queryFieldName()));
    }

    @Override
    public AggType getAggType() {
        return AggType.DATE_HISTOGRAM;
    }
}
