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
import com.ly.ckibana.model.compute.aggregation.bucket.TermsBucket;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.enums.TermsAggOrderType;
import com.ly.ckibana.model.request.CkRequest;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.strategy.aggs.converter.CountSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.FieldSqlConverter;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.SqlUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Component
@NoArgsConstructor
@Data
public final class TermsAggStrategy extends Aggregation {
    public static final String CK_STRING_DEFAULT_VALUE = "''";
    public static final int CK_NUMBER_DEFAULT_VALUE = 0;
    private TermsAggOrderType orderType;

    private String order;

    private int orderBySubAggIndex;

    private String missing;

    private TermsAggStrategy(AggsParam aggsParam) {
        super(aggsParam);
        setOrder(SqlUtils.getOrderString(SqlConstants.DEFAULT_COUNT_NAME, true));
        setOrderBySubAggIndex(SqlConstants.DEFAULT_SORT_BY_SUB_AGG_INDEX);
        setOrder(SqlUtils.getOrderString(queryAggCountName(), true));
        setOrderType(TermsAggOrderType.METRIC_COUNT);
        setSize(parseSizeField(aggsParam.getAggSetting()));
        setMissing(parseMissingField(aggsParam.getAggSetting()));
        parseTermsAggsSort(aggsParam);
    }

    @Override
    public TermsAggStrategy generate(final AggsParam aggsParam) {
        return new TermsAggStrategy(aggsParam);
    }

    @Override
    public List<SqlConverter> buildSelectSqlConvertors(Range timeRange) {
        List<SqlConverter> result = new ArrayList<>(2);
        FieldSqlConverter fieldAggregation = new FieldSqlConverter();
        fieldAggregation.setName(queryFieldName());
        fieldAggregation.setCondition(ProxyUtils.getFieldSqlPart(getField()));
        result.add(fieldAggregation);
        CountSqlConverter countAggregation = new CountSqlConverter();
        countAggregation.setCondition(StringUtils.EMPTY);
        countAggregation.setName(queryAggCountName());
        result.add(countAggregation);
        return result;
    }

    @Override
    public List<String> buildOrdersBySql() {
        return Lists.newArrayList(order);
    }

    @Override
    public void appendAggsConditions(CkRequest ckRequest, CkRequestContext ckRequestContext) {
        if (StringUtils.isBlank(missing)) {
            ckRequest.appendWhere(SqlUtils.getIsNotMissingSql(queryFieldName(), ProxyUtils.isString(getFieldType()) ? CK_STRING_DEFAULT_VALUE : CK_NUMBER_DEFAULT_VALUE));
        }
        super.appendAggsConditions(ckRequest, ckRequestContext);
    }

    @Override
    public CkRequest buildCkRequest(CkRequestContext ckRequestContext) {
        CkRequest result = super.buildCkRequest(ckRequestContext);
        if (getSize() == null) {
            setSize(10);
        }
        //若子aggs的有额外group by条件，size条件失效
        if (isSubAggGroupByEmpty()) {
            result.limit(getSize());
        }
        return result;
    }

    /**
     * 判断子aggs产生的group by条件是否为空.
     *
     * @return true 为空，false 不为空
     */
    private boolean isSubAggGroupByEmpty() {
        return CollectionUtils.isEmpty(getSubAggs()) || getSubAggs().stream().noneMatch(v -> CollectionUtils.isNotEmpty(v.buildGroupBySql()));
    }

    @Override
    public TermsBucket buildResultBucket(JSONObject obj) {
        TermsBucket result = new TermsBucket();
        Object termValue = obj.get(queryFieldName());
        boolean stringBlankValueCheck = ProxyUtils.isString(getFieldType()) && StringUtils.isBlank(termValue.toString());
        boolean numberOverflowValueCheck = ProxyUtils.isNumeric(getFieldType()) && String.valueOf(Constants.CK_NUMBER_DEFAULT_VALUE).equals(new BigDecimal(termValue.toString()).toString());
        boolean isSetMissingValue = termValue == null || stringBlankValueCheck || numberOverflowValueCheck;
        if (StringUtils.isNotBlank(missing) && isSetMissingValue) {
            termValue = missing;
        }
        result.setKey(termValue);
        result.setDocCount(obj.getLongValue(queryAggCountName()));
        return result;
    }

    @Override
    public AggType getAggType() {
        return AggType.TERMS;
    }

    @Override
    public List<String> buildGroupBySql() {
        return Lists.newArrayList(SqlUtils.escape(queryFieldName()));
    }

    private void parseTermsAggsSort(AggsParam aggsParam) {
        JSONObject aggSetting = aggsParam.getAggSetting();
        Map<String, String> columns = aggsParam.getColumns();
        List<Aggregation> subAggs = aggsParam.getSubAggs();
        JSONObject orderParam = aggSetting.getJSONObject(SqlConstants.ORDER);
        String orderSql = order;
        if (!aggSetting.containsKey(SqlConstants.ORDER)) {
            return;
        }
        if (orderParam != null) {
            String uiSortField = orderParam.keySet().iterator().next();
            if (SqlConstants.DEFAULT_COUNT_NAME.equals(uiSortField)) {
                orderSql = generateOrderSql(queryAggCountName(), uiSortField, orderParam);
                orderType = TermsAggOrderType.METRIC_COUNT;
            } else if (SqlConstants.KEY_NAME.equals(uiSortField)) {
                orderSql = generateOrderSql(getField(), uiSortField, orderParam);
                orderType = TermsAggOrderType.ALPHABETICAL;
            } else if (!columns.containsKey(uiSortField)) {
                orderSql = generateOrderSql(getMetricCustomSortField(subAggs, uiSortField), uiSortField, orderParam);
            } else {
                orderSql = ProxyUtils.getFieldSqlPart(uiSortField) + " " + orderParam.get(uiSortField);
            }
        }
        setOrder(orderSql);
    }

    private String getMetricCustomSortField(List<Aggregation> subAggs, String sortField) {
        String result = parseSubAggsAndGetField(subAggs, sortField);
        result = parsePeerAggsAndGetField(result);
        return result;
    }

    private String generateOrderSql(String ckField, String uiSortField, Map<String, Object> uiOrderParam) {
        String ckFieldName = ProxyUtils.getFieldSqlPart(ckField);
        String orderType = (String) uiOrderParam.get(uiSortField);
        return SqlUtils.getOrderString(ckFieldName, orderType);
    }

    private String parseSubAggsAndGetField(List<Aggregation> subAggs, String field) {
        String result = field;
        if (subAggs == null) {
            return result;
        }
        for (int i = 0; i < subAggs.size(); i++) {
            Aggregation o = subAggs.get(i);
            if (o.getAggName().equals(result)) {
                result = o.queryFieldName();
                orderType = TermsAggOrderType.METRIC_CUSTOM;
                orderBySubAggIndex = i;
                break;
            }
        }
        return result;
    }

    private String parsePeerAggsAndGetField(String field) {
        String result = field;
        if (getPeerAggs() == null) {
            return result;
        }
        for (int i = 0; i < getPeerAggs().size(); i++) {
            Aggregation sub = getPeerAggs().get(i);
            if (sub.getAggName().equals(result)) {
                result = sub.queryFieldName();
                orderType = TermsAggOrderType.PEER_AGG;
                orderBySubAggIndex = i;
                break;
            }
        }
        return result;
    }

    private Integer parseSizeField(JSONObject obj) {
        return obj.containsKey("size") ? obj.getIntValue("size") : 10;
    }

    private String parseMissingField(JSONObject obj) {
        return obj.containsKey("missing") ? obj.getString("missing") : null;
    }
}
