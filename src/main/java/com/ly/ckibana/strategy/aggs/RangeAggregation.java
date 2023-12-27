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

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.aggregation.AggsParam;
import com.ly.ckibana.model.compute.aggregation.RangeAggItem;
import com.ly.ckibana.model.compute.aggregation.bucket.RangeBucket;
import com.ly.ckibana.model.compute.aggregation.bucket.RangeItemBucket;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.request.CkRequest;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.strategy.aggs.converter.CountSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.SqlUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Component
@NoArgsConstructor
@Data
@Getter
public class RangeAggregation extends Aggregation {

    private List<RangeAggItem> ranges;

    private String fieldType;

    public RangeAggregation(AggsParam aggsParam) {
        super(aggsParam);
        fieldType = aggsParam.getFieldType();
        JSONArray array = aggsParam.getAggSetting().getJSONArray(Constants.RANGES);
        List<RangeAggItem> items = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            RangeAggItem aggItem = new RangeAggItem();
            JSONObject object = array.getJSONObject(i);
            aggItem.setFrom(object.get(Constants.RANGE_FROM));
            aggItem.setTo(object.get(Constants.RANGE_TO));
            String showFrom = (aggItem.getFrom() == null || StringUtils.isBlank(aggItem.getFrom().toString())) ? Constants.MATCH_ALL : aggItem.getFrom().toString();
            String showTo = (aggItem.getTo() == null || StringUtils.isBlank(aggItem.getTo().toString())) ? Constants.MATCH_ALL : aggItem.getTo().toString();
            aggItem.setKey(object.containsKey(Constants.KEY_NAME) ? object.getString(Constants.KEY_NAME) : (showFrom + Constants.RANGE_SPLIT + showTo));
            items.add(aggItem);
        }
        setRanges(items);
    }

    @Override
    public RangeAggregation generate(AggsParam aggsParam) {
        return new RangeAggregation(aggsParam);
    }

    @Override
    public List<SqlConverter> buildSelectSqlConvertors(Range timeRange) {
        String field = SqlUtils.escape(getField());
        List<SqlConverter> result = new ArrayList<>();

        for (RangeAggItem each : ranges) {
            CountSqlConverter countSqlConverter = new CountSqlConverter();
            String condition;
            if (each.getFrom() == null) {
                condition = SqlUtils.getCompareSql(field, Constants.Symbol.LT, each.getTo());
            } else if (each.getTo() == null) {
                condition = SqlUtils.getCompareSql(field, Constants.Symbol.GTE, each.getFrom());
            } else {
                String condition1 = SqlUtils.getCompareSql(field, Constants.Symbol.GTE, each.getFrom());
                String condition2 = SqlUtils.getCompareSql(field, Constants.Symbol.LT, each.getTo());
                condition = SqlUtils.getCompareSql(condition1, SqlConstants.AND, condition2);
            }
            if (StringUtils.isNotBlank(condition)) {
                countSqlConverter.setName(each.getKey());
                countSqlConverter.setCondition(condition);
            }
            result.add(countSqlConverter);
        }
        return result;
    }

    @Override
    public String queryFieldName() {
        return getField();
    }

    @Override
    public void appendAggsConditions(CkRequest ckRequest, CkRequestContext ckRequestContext) {
        if (ProxyUtils.isNumeric(getFieldType())) {
            ckRequest.appendWhere(SqlUtils.getNotEqualString(ProxyUtils.getFieldSqlPart(getField()), Constants.CK_NUMBER_DEFAULT_VALUE));
        }
        super.appendAggsConditions(ckRequest, ckRequestContext);
    }

    @Override
    public RangeBucket buildResultBucket(JSONObject obj) {
        RangeBucket result = new RangeBucket();
        result.setItems(new ArrayList<>());
        result.setDocCount(0L);
        ranges.forEach(each -> {
            RangeItemBucket itemBucket = getResultBucketItem(each.getKey(), obj);
            result.getItems().add(itemBucket);
            result.setDocCount(result.getDocCount() + itemBucket.getDocCount());
        });
        return result;
    }

    private RangeItemBucket getResultBucketItem(String rangeKey, JSONObject obj) {
        String from = rangeKey.split(Constants.RANGE_SPLIT)[0];
        String to = rangeKey.split(Constants.RANGE_SPLIT)[1];
        RangeItemBucket result = new RangeItemBucket();
        result.setKey(rangeKey);
        result.setDocCount(obj.getLongValue(rangeKey));
        if (ProxyUtils.isNumeric(getFieldType())) {
            boolean isDouble = ProxyUtils.isDouble(getFieldType());
            if (!isMatchAll(from)) {
                result.setFrom(isDouble ? Double.parseDouble(from) : Long.parseLong(from));
            }
            if (!isMatchAll(to)) {
                result.setTo(isDouble ? Double.parseDouble(to) : Long.parseLong(to));
            }
        } else {
            if (!isMatchAll(from)) {
                result.setFrom(from);
            }
            if (!isMatchAll(to)) {
                result.setTo(to);
            }
        }
        return result;
    }

    public boolean isMatchAll(String from) {
        return Constants.MATCH_ALL.equals(from);
    }

    @Override
    public AggType getAggType() {
        return AggType.RANGE;
    }
}
