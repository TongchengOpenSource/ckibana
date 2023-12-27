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
import com.google.common.base.Strings;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.compute.aggregation.AggsParam;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.enums.AggBucketsName;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.exception.UnSupportAggsTypeException;
import com.ly.ckibana.model.property.KibanaItemProperty;
import com.ly.ckibana.model.property.QueryProperty;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.response.IndexPatternFields;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.strategy.aggs.Aggregation;
import com.ly.ckibana.strategy.aggs.helper.FiltersAggsStrategyHelpler;
import com.ly.ckibana.util.DateUtils;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 解析参数类.
 *
 * @author quzhihao
 */
@Slf4j
@Service
public class ParamParser {

    private static final String KEYED = "keyed";

    private static final String FIELD = "field";

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Resource
    private CkService ckService;

    @Resource
    private List<Aggregation> aggStrategyList;

    @Resource
    private FiltersAggsStrategyHelpler filtersAggsStrategyHelpler;

    @Setter
    private Map<AggType, Aggregation> aggStrategyMap;


    @PostConstruct
    public void init() {
        aggStrategyMap = new HashMap<>(aggStrategyList.size());
        aggStrategyList.forEach(each -> aggStrategyMap.put(each.getAggType(), each));
    }

    /**
     * 基于入口参数，解析indexPattern信息.
     * 远程集群名（数据库__集群业务名称):索引名 如businesslog__log:anquan_dns-*
     * 兼容原有es业务，需要default库名
     */
    public IndexPattern buildIndexPattern(RequestContext context) {
        return context.getProxyConfig().buildIndexPattern(context.getOriginalIndex(), context.getIndex());
    }

    /**
     * 是否需要采样，部分index采样。默认不采样.
     *
     * @param uiIndex uiIndex
     * @return 是否需要采样
     */
    public boolean checkIfNeedSampleByIndex(String uiIndex) {
        QueryProperty queryProperty = proxyConfigLoader.getKibanaProperty().getQuery();
        if (queryProperty == null) {
            return false;
        }
        List<String> sampleIndexPatterns = queryProperty.getSampleIndexPatterns();
        if (CollectionUtils.isEmpty(sampleIndexPatterns)) {
            return false;
        }
        for (String each : sampleIndexPatterns) {
            if (uiIndex.equals(each)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 将查询语句转换为对应的agg聚合策略.
     */
    public List<Aggregation> parseAggs(int depth, CkRequestContext ckRequestContext, JSONObject searchQuery) {
        List<Aggregation> result = null;
        if (searchQuery.containsKey(Constants.AGGS)) {
            result = new ArrayList<>();
            JSONObject aggs = searchQuery.getJSONObject(Constants.AGGS);
            for (String aggsKey : aggs.keySet()) {
                List<Aggregation> subAggs = null;
                JSONObject aggsSettings = aggs.getJSONObject(aggsKey);
                if (aggsSettings.containsKey(Constants.AGGS)) {
                    subAggs = parseAggs(depth + 1, ckRequestContext, aggsSettings);
                }
                List<Aggregation> currentAggs = doParseAggs(depth, aggsKey, ckRequestContext, aggsSettings, subAggs);
                if (currentAggs.isEmpty()) {
                    continue;
                }
                if (aggsSettings.containsKey(Constants.AGGS) && CollectionUtils.isNotEmpty(subAggs)) {
                    for (Aggregation currentAgg : currentAggs) {
                        currentAgg.setSubAggs(subAggs);
                    }
                }
                result.addAll(currentAggs);
            }
        }
        return result;
    }

    /**
     * 基于agg类型解析出对应的object.
     */
    private List<Aggregation> doParseAggs(int depth, String aggName, CkRequestContext ckRequestContext, JSONObject parentSetting, List<Aggregation> subAggs) {
        List<Aggregation> aggs = new ArrayList<>();
        AggsParam baseAggsParam = new AggsParam(aggName, depth, ckRequestContext.getQuery(), ckRequestContext.getColumns(), subAggs);
        for (String each : parentSetting.keySet()) {
            if (Constants.AGGS.equals(each)) {
                continue;
            }
            AggsParam aggsParam = buildAggParam(baseAggsParam, each, parentSetting.getJSONObject(each));
            AggType type = aggsParam.getAggType();
            Aggregation aggregation = aggStrategyMap.get(type);
            if (null == aggregation) {
                throw new UnSupportAggsTypeException(each, AggType.getAggTypes());
            }
            if (AggType.FILTERS.equals(type)) {
                aggs.add(filtersAggsStrategyHelpler.generate(aggsParam, ckRequestContext));
            } else {
                aggs.add(aggregation.generate(aggsParam));
            }
        }
        return aggs;
    }

    private AggsParam buildAggParam(AggsParam baseAggsParam, String aggType, JSONObject aggSetting) {
        AggsParam result = JSONUtils.copy(baseAggsParam);
        String aggField = aggSetting.getString(FIELD);
        Map<String, String> supportColumns = baseAggsParam.getColumns();
        if (!validAggField(aggType, aggField, supportColumns)) {
            throw new UnSupportAggsTypeException(aggType.toUpperCase(), aggField, AggType.getAggTypes());
        }
        try {
            result.setAggType(AggType.valueOf(aggType.toUpperCase()));
            result.setAggBucketsName(getAggBucketsNameByAggType(result));
        } catch (Exception e) {
            throw new UnSupportAggsTypeException(aggType.toUpperCase(), aggField, AggType.getAggTypes());
        }
        String fieldType = supportColumns.get(aggField);
        result.setAggSetting(aggSetting);
        result.setField(aggField);
        result.setKeyed(aggSetting.getBooleanValue(KEYED));
        result.setFieldType(fieldType);
        return result;
    }

    private AggBucketsName getAggBucketsNameByAggType(AggsParam result) {
        return (AggType.PERCENTILES.equals(result.getAggType()) || AggType.PERCENTILE_RANKS.equals(result.getAggType()))
                ? AggBucketsName.VALUES : AggBucketsName.BUCKETS;
    }

    private boolean validAggField(String aggType, String aggField, Map<String, String> supportColumns) {
        return isFiltersAgg(aggType) || !isMissAggField(aggField) && !isMissAggFieldColumn(aggField, supportColumns);
    }

    /**
     * 是否ck列中缺失aggField字段.
     *
     * @param aggField       aggField
     * @param supportColumns supportColumns
     * @return 是否ck列中缺失的字段
     */
    private boolean isMissAggFieldColumn(String aggField, Map<String, String> supportColumns) {
        return !Constants.ES_INDEX_QUERY_FIELD.equals(aggField) && !supportColumns.containsKey(aggField);
    }

    /**
     * 是否用户参数缺失aggField.
     *
     * @param aggField aggField
     * @return boolean
     */
    private boolean isMissAggField(String aggField) {
        return StringUtils.isEmpty(aggField);
    }

    /**
     * 是否为filters聚合.
     *
     * @param aggType aggType
     * @return boolean
     */
    private boolean isFiltersAgg(String aggType) {
        return AggType.FILTERS.name().toLowerCase().equals(aggType);
    }

    /**
     * 查询列信息.
     */
    public Map<String, String> queryTableColumns(BalancedClickhouseDataSource clickhouseDataSource, String tableName) throws Exception {
        return ckService.queryColumns(clickhouseDataSource, tableName);
    }

    /**
     * 如果需要走 cache，cache 返回不为空再请求es.
     */
    public Map<String, String> queryColumnsFromCache(Map<String, Map<String, String>> cache, String tableName) {
        Map<String, String> result = null;
        //不为空不为*的table名称才需要查询列名
        if (Strings.isNullOrEmpty(tableName) || ProxyUtils.matchAllIndex(tableName)) {
            return new HashMap<>();
        }
        if (cache != null && cache.containsKey(tableName)) {
            result = cache.get(tableName);
        }
        return result;
    }

    /**
     * 基于indexPattern，查询其下的字段，包括名称和类型.
     *
     * @param requestContext       请求上下文
     * @param indexPattern         当前index pattern
     * @param isForSelectTimeField 是:代表为创建index pattern时间时选择时间字段列表场景，需要额外类型转换（规则参见ProxyUtils.isTimeFieldOptionByName）
     *                             否:代表查询index pattern字段列表。无需额外类型转换
     */
    public Map<String, JSONObject> queryIndexPatternFields(RequestContext requestContext, IndexPattern indexPattern,
                                                           boolean isForSelectTimeField) throws Exception {
        Map<String, JSONObject> result = new HashMap<>();
        String tableName = indexPattern.getIndex();
        Map<String, String> columns = ckService.queryColumns(requestContext.getProxyConfig().getCkDatasource(), tableName);
        //特殊查询字段不存在，转为真实对应的ck字段
        for (Map.Entry<String, String> each : columns.entrySet()) {
            String ckName = each.getKey();
            String ckType = each.getValue();
            //当isForSelectTimeField为true情况，若indexPattern已经设置时间字段，则非时间字段按原始类型返回即可。
            String esType = ProxyUtils.convertCkTypeToEsType(ckName, ckType,
                    isNotIndexPatternTimeField(indexPattern, ckName) ? Boolean.FALSE : isForSelectTimeField);
            JSONObject jsonObjectType = new JSONObject();
            jsonObjectType.put(esType, new IndexPatternFields(ckName, esType));
            result.put(ckName, jsonObjectType);
        }
        return result;
    }

    /**
     * 是否命中已经被设置为时间字段的字段名.
     *
     * @param indexPattern 当前index pattern
     * @param ckName       ck字段名
     * @return boolean
     */
    private boolean isNotIndexPatternTimeField(IndexPattern indexPattern, String ckName) {
        return StringUtils.isNotBlank(indexPattern.getTimeField()) && !ckName.equals(indexPattern.getTimeField());
    }

    /**
     * 计算用户选择的时间框间隔ms.
     *
     * @param timeRange range时间范围
     */
    public long getTimeIntervalMillSeconds(Range timeRange) {
        return timeRange == null ? 0L : timeRange.getDiffMillSeconds();
    }

    /**
     * 时间参数round.
     * 2分钟内数据不round,inSpecialTime round 20s,默认round 10s
     * 返回ui
     */
    public Range getTimeRangeAfterRound(CkRequestContext ckRequestContext) {
        Range result = ckRequestContext.getTimeRange();
        KibanaItemProperty kibanaItemProperty = proxyConfigLoader.getKibanaProperty().getProxy();
        boolean isRoundAble = getTimeIntervalMillSeconds(result) > kibanaItemProperty.getRoundAbleMinPeriod();
        if (isRoundAble) {
            int realRound = Constants.ROUND_SECOND;
            result.setHigh(DateUtils.roundToMSecond(Math.min(System.currentTimeMillis(), (Long) result.getHigh()), realRound));
            result.setLow(DateUtils.roundToMSecond((Long) result.getLow(), realRound));
        }
        return result;
    }

    /**
     * 查询代理配置MaxResultRow.
     *
     * @return MaxResultRow
     */
    public int getMaxResultRow() {
        return this.proxyConfigLoader.getKibanaProperty().getQuery().getMaxResultRow();
    }
}
