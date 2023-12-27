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
import com.ly.ckibana.model.compute.aggregation.bucket.Bucket;
import com.ly.ckibana.model.enums.AggBucketsName;
import com.ly.ckibana.model.enums.AggCategory;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.request.CkRequest;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.CkRequestContext.SampleParam;
import com.ly.ckibana.strategy.aggs.converter.SqlConverter;
import com.ly.ckibana.util.ProxyUtils;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 聚合策略.
 *
 * @author zl
 */
@Data
public class Aggregation {

    private AggType aggType;

    private String aggName;

    /**
     * query_string中的查询条件.
     */
    private String commonQuery;

    /**
     * true返回hash map格式，否则返回list格式.
     */
    private boolean keyed;

    /**
     * 用于agg数据内存计算.
     */
    private int aggDepth;

    /**
     * 下一层aggs.
     */
    private List<Aggregation> subAggs;

    /**
     * 同一层aggs.
     */
    private List<Aggregation> peerAggs;

    private Integer size;

    /**
     * 聚合的列.
     */
    private String field;

    /**
     * 聚合的列的ck类型.
     */
    private String fieldType;

    /**
     * 聚合所属的大类，用于计算区分.
     */
    private AggCategory aggCategory = AggCategory.NOT_MATH;

    /**
     * 聚合结果返回map的字段名.
     */
    private AggBucketsName aggBucketsName;

    public Aggregation() {
    }

    public Aggregation(AggType aggType, String aggName, String commonQuery, boolean keyed, int aggDepth,
                       String field, String fieldType, AggBucketsName aggBucketsName) {
        this.aggType = aggType;
        this.aggName = aggName;
        this.commonQuery = commonQuery;
        this.keyed = keyed;
        this.aggDepth = aggDepth;
        this.field = field;
        this.fieldType = fieldType;
        this.aggBucketsName = aggBucketsName;
    }

    /**
     * 从agg配置构建aggStrategy.
     */
    public Aggregation(AggsParam aggsParam) {
        this(aggsParam.getAggType(), aggsParam.getAggName(), aggsParam.getCommonQuery(),
                aggsParam.isKeyed(), aggsParam.getDepth(), aggsParam.getField(), aggsParam.getFieldType(), aggsParam.getAggBucketsName());
    }

    public Aggregation generate(AggsParam aggsParam) {
        return new Aggregation(aggsParam);
    }

    /**
     * 聚合中field对应的alias.
     *
     * @return String
     */
    public String queryFieldName() {
        return getAggName() + SqlConstants.QUERY_NAME_SEPARATOR + getField();
    }

    /**
     * 聚合中count对应的alias.
     *
     * @return String
     */
    public String queryAggCountName() {
        return getAggName() + SqlConstants.QUERY_NAME_SEPARATOR + SqlConstants.CK_COUNT_NAME;
    }

    /**
     * 解析对应agg对应的bucket结构.
     *
     * @param obj obj
     * @return bucket
     */
    public Bucket buildResultBucket(JSONObject obj) {
        return null;
    }

    /**
     * 解析用于拼接ck sql的各部分sql part.
     *
     * @param ckRequestContext ckRequestContext
     * @return CkRequest
     */
    public CkRequest buildCkRequest(CkRequestContext ckRequestContext) {
        CkRequest result = ProxyUtils.buildCkRequest(ckRequestContext);
        buildGroupSql(result);
        buildOrderBySql(result);
        double sample = getSample(ckRequestContext.getSampleParam());
        buildSelectSql(sample, result, ckRequestContext);
        buildWhereSql(result, ckRequestContext, sample);
        //按代理配置设置默认limit
        if (StringUtils.isEmpty(result.getLimit()) && ckRequestContext.getMaxResultRow() > 0) {
            result.limit(ckRequestContext.getMaxResultRow());
        }

        return result;
    }
    
    /**
     * 获取采样率.
     */
    private double getSample(SampleParam sampleParam) {
        double sample = Constants.DEFAULT_NO_SAMPLE;
        if (sampleParam != null && sampleParam.getSampleTotalCount() > sampleParam.getSampleCountMaxThreshold()) {
            sample = Math.max(0.01, Double.parseDouble(String.format("%.5f", sampleParam.getSampleCountMaxThreshold() * 1.00 / sampleParam.getSampleTotalCount())));
        }
        return sample;
    }

    /**
     * 计算group sql.
     *
     * @param ckRequest ckRequest
     */
    private void buildGroupSql(CkRequest ckRequest) {
        List<String> groupBy = collectGroupBy();
        ckRequest.initGroupBy(String.join(",", groupBy));
    }

    /**
     * 解析group by.
     *
     * @return List
     */
    public List<String> collectGroupBy() {
        List<String> result = new ArrayList<>(buildGroupBySql());
        if (CollectionUtils.isNotEmpty(getPeerAggs())) {
            for (Aggregation each : getPeerAggs()) {
                result.addAll(each.collectGroupBy());
            }
        }
        if (CollectionUtils.isNotEmpty(getSubAggs()) && !isIgnoreSubAggCondition()) {
            for (Aggregation each : getSubAggs()) {
                result.addAll(each.collectGroupBy());
            }
        }
        return result;
    }

    /**
     * 目前仅针对range+子terms，terms+子terms.
     *
     * @return boolean
     */
    public boolean isIgnoreSubAggCondition() {
        return (AggType.RANGE.equals(this.aggType) || AggType.TERMS.equals(this.aggType))
                && CollectionUtils.isNotEmpty(getSubAggs())
                && getSubAggs().stream().filter(v -> AggType.TERMS.equals(v.getAggType())).count() > 0;
    }

    public List<String> buildGroupBySql() {
        return new ArrayList<>();
    }

    /**
     * 计算select.
     *
     * @param ckRequest        ckRequest
     * @param ckRequestContext ckRequestContext
     */
    private void buildSelectSql(double sample, CkRequest ckRequest, CkRequestContext ckRequestContext) {
        List<SqlConverter> convertors = new ArrayList<>();
        buildSelectSqlConvertors(convertors, ckRequestContext.getTimeRange());
        ckRequest.initSelect(buildRealSelectSql(sample, convertors));
        ckRequest.appendSelect(null);
    }

    /**
     * 收集select 转换器.
     *
     * @param aggregations aggregations
     * @param timeRange    timeRange
     */
    public void buildSelectSqlConvertors(List<SqlConverter> aggregations, Range timeRange) {
        if (CollectionUtils.isNotEmpty(getPeerAggs())) {
            for (Aggregation each : getPeerAggs()) {
                each.buildSelectSqlConvertors(aggregations, timeRange);
            }
        }
        if (CollectionUtils.isNotEmpty(getSubAggs()) && !isIgnoreSubAggCondition()) {
            for (Aggregation each : getSubAggs()) {
                each.buildSelectSqlConvertors(aggregations, timeRange);
            }
        }
        aggregations.addAll(buildSelectSqlConvertors(timeRange));
    }

    /**
     * 解析对应的用于转换sql的aggregations.
     *
     * @param timeRange timeRange
     * @return List
     */
    public List<SqlConverter> buildSelectSqlConvertors(Range timeRange) {
        return new ArrayList<>();
    }

    /**
     * 聚合object->sql.
     *
     * @param aggs aggs
     * @return sql
     */
    public String buildRealSelectSql(double sample, List<SqlConverter> aggs) {
        if (CollectionUtils.isEmpty(aggs)) {
            return "";
        }
        return aggs.stream().map(v -> v.toSql(sample)).collect(Collectors.joining(","));
    }

    /**
     * 计算where.
     *
     * @param ckRequest        ckRequest
     * @param ckRequestContext ckRequestContext
     * @param sample           sample
     */
    private void buildWhereSql(CkRequest ckRequest, CkRequestContext ckRequestContext, double sample) {
        appendUserQueryConditions(ckRequest, ckRequestContext);
        appendAggsConditions(ckRequest, ckRequestContext);
        if (sample < Constants.DEFAULT_NO_SAMPLE) {
            ckRequest.setSample(String.format("sample %s", sample));
        }
    }

    private void appendUserQueryConditions(CkRequest ckRequest, CkRequestContext ckRequestContext) {
        ckRequest.appendWhere(ckRequestContext.getQuery());
    }

    /**
     * 解析where 条件.
     *
     * @param ckRequest        ckRequest
     * @param ckRequestContext ckRequestContext
     */
    public void appendAggsConditions(CkRequest ckRequest, CkRequestContext ckRequestContext) {
        if (CollectionUtils.isNotEmpty(getPeerAggs())) {
            for (Aggregation each : getPeerAggs()) {
                each.appendAggsConditions(ckRequest, ckRequestContext);
            }
        }
        if (CollectionUtils.isNotEmpty(getSubAggs()) && !isIgnoreSubAggCondition()) {
            for (Aggregation each : getSubAggs()) {
                each.appendAggsConditions(ckRequest, ckRequestContext);
            }
        }
    }

    /**
     * 计算order by.
     *
     * @param ckRequest ckRequest
     */
    private void buildOrderBySql(CkRequest ckRequest) {
        List<String> orderBy = collectorOrders();
        ckRequest.orderBy(String.join(",", orderBy));
    }

    /**
     * 解析order by.
     *
     * @return List
     */
    public List<String> collectorOrders() {
        List<String> result = new ArrayList<>(buildOrdersBySql());
        if (CollectionUtils.isNotEmpty(getPeerAggs())) {
            for (Aggregation each : getPeerAggs()) {
                result.addAll(each.collectorOrders());
            }
        }
        if (CollectionUtils.isNotEmpty(getSubAggs()) && !isIgnoreSubAggCondition()) {
            for (Aggregation each : getSubAggs()) {
                result.addAll(each.collectorOrders());
            }
        }
        return result;
    }

    public List<String> buildOrdersBySql() {
        return new ArrayList<>();
    }

    public String getStaticsId() {
        return "depth" + getAggDepth() + "name" + getAggName();
    }
}
