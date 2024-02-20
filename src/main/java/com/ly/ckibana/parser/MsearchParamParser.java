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

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Strings;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.enums.SortType;
import com.ly.ckibana.model.exception.TimeNotInRangeException;
import com.ly.ckibana.model.exception.UnKnowTimeFieldException;
import com.ly.ckibana.model.property.KibanaItemProperty;
import com.ly.ckibana.model.property.QueryProperty;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.CkRequestContext.SampleParam;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.request.SortedField;
import com.ly.ckibana.model.response.DocValue;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.strategy.clause.ClauseStrategySelector;
import com.ly.ckibana.util.DateUtils;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ParamConvertUtils;
import com.ly.ckibana.util.ProxyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * msearch参数解析类.
 *
 * @author caojiaqiang
 */
@Slf4j
@Service
public class MsearchParamParser extends ParamParser {

    @Resource
    private AggResultParser aggResultParser;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Resource
    private ClauseStrategySelector clauseStrategySelector;

    public void checkTimeInRange(CkRequestContext ckRequestContext) {
        if (!isTimeInRange(ckRequestContext)) {
            throw new TimeNotInRangeException("查询时间跨度太大,目前支持最大查询区间为:"
                                              + DateUtils.formatDurationWords(proxyConfigLoader.getKibanaProperty().getProxy().getMaxTimeRange()));
        }
    }

    public void convert2WhereSqlWithTimeRound(JSONObject searchQuery, CkRequestContext ckRequestContext) {
        // 将业务条件转换为sql
        String querySql = buildQuerySqlWithoutTimeRange(searchQuery, ckRequestContext);
        ckRequestContext.setQuerySqlWithoutTimeRange(querySql);
        // round时间参数
        ckRequestContext.setTimeRange(getTimeRangeAfterRound(ckRequestContext));
        // 查询条件+round后的时间
        String otherQuery = ckRequestContext.getTimeRange() == null ? "" : ckRequestContext.getTimeRange().toSql(true);
        ckRequestContext.setQuery(appendWhereSql(querySql, otherQuery));
    }

    /**
     * 解析query条件，bool逻辑表达式和单个的query.
     */
    private String buildQuerySqlWithoutTimeRange(JSONObject searchQuery, CkRequestContext ckRequestContext) {
        String result = "";
        if (searchQuery.containsKey(Constants.QUERY)) {
            JSONObject queryPara = searchQuery.getJSONObject(Constants.QUERY);
            result = clauseStrategySelector.buildQuerySql(ckRequestContext, queryPara);
        }
        return result;
    }

    private String appendWhereSql(String query, String otherQuery) {
        if (Strings.isNullOrEmpty(query)) {
            return otherQuery;
        }
        if (Strings.isNullOrEmpty(otherQuery)) {
            return query;
        }
        return query + String.format(" AND (%s)", otherQuery);
    }

    /**
     * 用户时间框是否间隔过大.
     *
     * @param ckRequestContext ckRequestContext
     * @return boolean
     */
    private boolean isTimeInRange(CkRequestContext ckRequestContext) {
        KibanaItemProperty kibanaItemProperty = proxyConfigLoader.getKibanaProperty().getProxy();
        if (kibanaItemProperty == null || kibanaItemProperty.getMaxTimeRange() <= 0) {
            return true;
        }
        boolean result = true;
        if (ckRequestContext != null && ckRequestContext.getTimeRange() != null) {
            long diffMs = getTimeIntervalMillSeconds(ckRequestContext.getTimeRange());
            result = diffMs <= kibanaItemProperty.getMaxTimeRange();
        }
        return result;
    }

    /**
     * 解析出sort条件，得到a DESC,b ASC格式.
     * 同时解析得到sort涉及的fields(sortFields),用于hits response返回 和查询优化策略
     *
     * @param searchQuery      searchQuery
     * @param ckRequestContext ckRequestContext
     */
    public void parseSort(JSONObject searchQuery, CkRequestContext ckRequestContext) {
        List<SortedField> sortedFields = new ArrayList<>();
        List<String> sort = new ArrayList<>();
        if (searchQuery.containsKey(Constants.SORT)) {
            List<Map> sortArray = JSONUtils.deserializeToList(searchQuery.getString(Constants.SORT), Map.class);
            for (Map<String, Object> each : sortArray) {
                each.keySet().forEach(orgField -> {
                    Map<String, String> columns = ckRequestContext.getColumns();
                    String ckSortFieldName = ParamConvertUtils.convertUiFieldToCkField(columns, orgField);
                    // 如果该字段在ck中不存在，且无ck_assembly_extension扩展字段，则不放到排序条件中
                    if (!columns.containsKey(ckSortFieldName) && !columns.containsKey(Constants.CK_EXTENSION)) {
                        return;
                    }
                    String ckSortType = StringUtils.upperCase(JSONUtils.convert(each.get(orgField), JSONObject.class).getString("order"));
                    sort.add(String.format("%s %s", ProxyUtils.getFieldSqlPart(ckSortFieldName), ckSortType));
                    sortedFields.add(new SortedField(orgField, ckSortFieldName, SortType.valueOf(ckSortType)));
                });
            }
        }
        ckRequestContext.setSortingFields(sortedFields);
        ckRequestContext.setSort(String.join(",", sort));
    }

    /**
     * 解析docValueField用于hits response解析为用户所需格式.
     */
    public List<DocValue> parseDocValueFields(JSONObject searchQuery, Map<String, String> columns, IndexPattern indexPattern) {
        List<DocValue> result = new ArrayList<>();
        if (!searchQuery.containsKey(Constants.DOC_VALUE_FIELDS)) {
            return result;
        }
        JSONArray docValueFields = searchQuery.getJSONArray(Constants.DOC_VALUE_FIELDS);
        for (int i = 0; i < docValueFields.size(); i++) {
            DocValue valueField = new DocValue();
            if (docValueFields.get(i).getClass().getSimpleName().equals(JSONObject.class.getSimpleName())) {
                JSONObject docValueFieldTemp = docValueFields.getJSONObject(i);
                valueField.setFieldName(docValueFieldTemp.getString(SqlConstants.FIELD));
                valueField.setCkFieldName(ParamConvertUtils.convertUiFieldToCkField(columns, valueField.getFieldName()));
                valueField.setFormat(docValueFieldTemp.getString(SqlConstants.FORMAT));
            } else {
                valueField.setFieldName(docValueFields.getString(i));
                valueField.setCkFieldName(ParamConvertUtils.convertUiFieldToCkField(columns, valueField.getFieldName()));
                if (valueField.getCkFieldName().equals(indexPattern.getTimeField())) {
                    valueField.setFormat(SqlConstants.DATE_TIME);
                }
            }
            result.add(valueField);
        }
        return result;
    }

    /**
     * 多线程处理msearch请求，得到返回结果列表.
     *
     * @param context           context
     * @param defaultIndex      defaultIndex
     * @param subEsRequest      subEsRequest
     * @param subCkRequests     subCkRequests
     * @param fastFailResponses fastFailResponses
     * @throws UnKnowTimeFieldException 异常
     */
    public void parse(RequestContext context, String defaultIndex, StringBuilder subEsRequest,
                      List<Callable<Response>> subCkRequests, List<Response> fastFailResponses) {
        String[] lines = context.getRequestInfo().getRequestBody().split(System.lineSeparator());
        QueryProperty queryProperty = proxyConfigLoader.getKibanaProperty().getQuery();
        ProxyConfig proxyConfig = context.getProxyConfig();
        Map<String, String> indexPatternMeta = EsClientUtil.getIndexPatternMeta(context.getProxyConfig().getRestClient());
        Map<String, Map<String, String>> tableColumnsCache = new HashMap<>();
        Map<String, Long> totalCountByQueryCache = new HashMap<>();
        for (int i = 0; i < lines.length; i++) {
            if (0 != i % 2) {
                continue;
            }
            String uiIndex = JSONUtils.deserialize(lines[i], JSONObject.class).getString(Constants.INDEX_NAME_KEY);
            if (uiIndex == null) {
                uiIndex = defaultIndex;
            }
            JSONObject searchQuery = JSONUtils.deserialize(lines[i + 1], JSONObject.class);
            try {
                if (proxyConfig.isDirectToEs(uiIndex)) {
                    subEsRequest.append(lines[i]).append(System.lineSeparator());
                    subEsRequest.append(lines[i + 1]).append(System.lineSeparator());
                    continue;
                }
                IndexPattern indexPattern = proxyConfig.buildIndexPattern(uiIndex);
                CkRequestContext ckRequestContext = new CkRequestContext(context.getClientIp(), indexPattern, queryProperty.getMaxResultRow());
                String timeField = indexPatternMeta.get(uiIndex);
                if (timeField == null) {
                    throw new UnKnowTimeFieldException(uiIndex);
                }
                parseRequestBySearchQuery(tableColumnsCache, searchQuery, timeField, indexPattern, ckRequestContext);
                if (checkIfNeedSampleByIndex(uiIndex)) {
                    CkRequestContext.SampleParam sampleParam = new SampleParam(Constants.USE_SAMPLE_COUNT_THREASHOLD, queryProperty.getSampleCountMaxThreshold());
                    sampleParam.setSampleTotalCount(getTotalCountBySearchQuery(totalCountByQueryCache, ckRequestContext));
                    ckRequestContext.setSampleParam(sampleParam);
                }
                // 将查询任务放入线程队列
                MsearchQueryTask msearchQueryTask = new MsearchQueryTask(ckRequestContext, aggResultParser, searchQuery);
                subCkRequests.add(msearchQueryTask);
            } catch (Exception ex) {
                log.error("msearch param parse error, i:{}, uiIndex:{}, searchQuery:{}", i, uiIndex, searchQuery, ex);
                fastFailResponses.add(ProxyUtils.newKibanaException(ex.getMessage()));
            }
        }

    }

    /**
     * 基于当前searchQuery获取totalCount.
     *
     * @param totalCountByQuery totalCountByQuery
     * @param ckRequestContext  ckRequestContext
     * @throws Exception 异常
     */
    private Long getTotalCountBySearchQuery(Map<String, Long> totalCountByQuery, CkRequestContext ckRequestContext) throws Exception {
        String userQuery = ckRequestContext.getQuerySqlWithoutTimeRange();
        if (totalCountByQuery.getOrDefault(userQuery, Constants.DEFAULT_TOTAL_COUNT) <= Constants.DEFAULT_TOTAL_COUNT) {
            totalCountByQuery.put(userQuery, aggResultParser.queryTotalCount(ckRequestContext, null));
        }
        return totalCountByQuery.get(userQuery);
    }

    /**
     * 基于searchQuery参数解析ckRequestContext的aggregation,docvalue_filed等.
     */
    public void parseRequestBySearchQuery(Map<String, Map<String, String>> tableColumnsCache,
                                          JSONObject searchQuery,
                                          String timeField,
                                          IndexPattern indexPattern,
                                          CkRequestContext ckRequestContext) throws Exception {
        // 解析es index->ck table+mid name,columns
        String tableName = indexPattern.getIndex();
        ckRequestContext.setTableName(tableName);
        Map<String, String> columns = queryColumnsFromCache(tableColumnsCache, tableName);
        if (columns == null) {
            columns = queryTableColumns(proxyConfigLoader.getConfig().getCkDatasource(), tableName);
            tableColumnsCache.put(tableName, columns);
        }
        ckRequestContext.setColumns(columns);

        // 解析msearch indexpattern ,额外需要timefield
        ckRequestContext.getIndexPattern().setTimeField(ParamConvertUtils.convertUiFieldToCkField(ckRequestContext.getColumns(), timeField));

        convert2WhereSqlWithTimeRound(searchQuery, ckRequestContext);
        checkTimeInRange(ckRequestContext);

        // 解析size,sort,docValue,agg条件
        ckRequestContext.setSize(searchQuery.containsKey(Constants.SIZE) ? searchQuery.getIntValue(Constants.SIZE) : 0);
        parseSort(searchQuery, ckRequestContext);
        ckRequestContext.setDocValues(parseDocValueFields(searchQuery, ckRequestContext.getColumns(), ckRequestContext.getIndexPattern()));
        ckRequestContext.setAggs(parseAggs(Constants.AGG_INIT_DEPTH, ckRequestContext, searchQuery));
    }
}
