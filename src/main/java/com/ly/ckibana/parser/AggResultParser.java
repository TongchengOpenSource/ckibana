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
import com.ly.ckibana.model.enums.AggBucketsName;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.request.CkRequest;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.strategy.aggs.Aggregation;
import com.ly.ckibana.strategy.aggs.FiltersAggregation;
import com.ly.ckibana.util.JSONUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 封装结果类.
 *
 * @author caojiaqiang
 */
@Service
public class AggResultParser extends HitsResultParser {

    @Resource
    private CkService ckService;

    @Resource
    private ResultParser resultParseService;
    
    @Resource
    private AggResultParserHelper aggResultParserHelper;

    /**
     * todo range子agg 请求1->n+1,filters 1->n待优化
     * 基于查询参数解析获取聚合结果.
     * filters是和其他agg组合使用。支持n个filter，每种filter有自己的query条件。filters会返回n组数据返回。
     * 其他聚合进返回1组数据
     *
     * @param ckRequestContext ckRequestContext
     * @return Response
     * @throws Exception 异常
     */
    public Response executeByAgg(CkRequestContext ckRequestContext) throws Exception {
        List<Aggregation> filtersAggList = ckRequestContext.getAggs().stream().filter(each -> AggType.FILTERS.equals(each.getAggType())).toList();
        if (!filtersAggList.isEmpty()) {
            return executeFiltersAggs(ckRequestContext, (FiltersAggregation) filtersAggList.get(0));
        } else {
            return executeNotFiltersAggs(ckRequestContext);
        }
    }

    /**
     * filters是和其他agg组合使用。支持n个filtersItem.
     * 1个filtersItem中包含一个query条件,此query条件作用于subAgg，返回1组执行结果。
     * filters结果为n个filtersItem*subAgg的结果并集。
     *
     * @param ckRequestContext ckRequestContext
     * @param filtersAgg       filtersAgg
     * @return Response
     * @throws Exception 异常
     */
    private Response executeFiltersAggs(CkRequestContext ckRequestContext, FiltersAggregation filtersAgg) throws Exception {
        checkMultiFiltersSupported(filtersAgg);
        Response result = new Response();
        Map<String, Map<String, Object>> filterAggResult = new HashMap<>();
        List<Aggregation> subAggs = filtersAgg.getSubAggs();
        for (int i = 0; i < filtersAgg.getFiltersItems().size(); i++) {
            Aggregation firstAggs = filtersAgg.getFiltersItems().get(i);
            firstAggs.setSubAggs(subAggs);
            CkRequest ckRequest = firstAggs.buildCkRequest(ckRequestContext);
            Response response = executeByAgg(ckRequestContext, firstAggs, ckRequest);
            if (response.getAggregations() != null) {
                filterAggResult.putAll(response.getAggregations());
            }
            result.getSqls().addAll(response.getSqls());
        }
        Map<String, Object> buckets = Collections.singletonMap(AggBucketsName.BUCKETS.name().toLowerCase(), filterAggResult);
        result.setAggregations(Collections.singletonMap(filtersAgg.getAggName(), buckets));
        return result;
    }

    private void checkMultiFiltersSupported(Aggregation filtersAgg) {
        if (filtersAgg.getSubAggs() != null) {
            for (Aggregation child : filtersAgg.getSubAggs()) {
                if (child instanceof FiltersAggregation) {
                    throw new UnsupportedOperationException("不支持多层filters聚合");
                }
                checkMultiFiltersSupported(child);
            }
        }
    }

    /**
     * 非filters查询.
     * @param ckRequestContext ckRequestContext
     * @return Response
     * @throws Exception 异常
     */
    private Response executeNotFiltersAggs(CkRequestContext ckRequestContext) throws Exception {
        Aggregation firstAgg = buildFirstAgg(ckRequestContext.getAggs());
        CkRequest ckRequest = firstAgg.buildCkRequest(ckRequestContext);
        Response response = executeByAgg(ckRequestContext, firstAgg, ckRequest);
        mergeSubResponses(ckRequestContext, firstAgg, response);
        return response;

    }

    /**
     * range agg可能包含子agg，需要转为多次查询结果拼接.
     * todo 待优化
     *
     * @param ckRequestContext ckRequestContext
     * @param parentAgg       parentAgg
     * @param response        response
     * @throws Exception 异常
     */
    public void mergeSubResponses(CkRequestContext ckRequestContext, Aggregation parentAgg, Response response) throws Exception {
        if (parentAgg.isIgnoreSubAggCondition()) {
            Object parentBuckets = response.getAggregations().get(parentAgg.getAggName()).get(AggBucketsName.BUCKETS.name().toLowerCase());
            Map<Object, String> appendWhereSqlForSub = aggResultParserHelper.buildAppendWhereForSubAgg(parentAgg, parentBuckets);
            //map类结果
            if (parentAgg.isKeyed()) {
                JSONObject mapBuckets = JSONUtils.convert(parentBuckets, JSONObject.class);
                for (int i = 0; i < parentAgg.getSubAggs().size(); i++) {
                    Aggregation aggregation = parentAgg.getSubAggs().get(i);
                    for (Map.Entry<Object, String> parentItem : appendWhereSqlForSub.entrySet()) {
                        CkRequest subAggCkRequest = aggResultParserHelper.getSubAggCkRequestByParentAgg(ckRequestContext, parentAgg,
                                aggregation, parentItem.getValue());
                        Response responseChild = executeByAgg(ckRequestContext, aggregation, subAggCkRequest);
                        mapBuckets.getJSONObject(parentItem.getKey().toString()).putAll(responseChild.getAggregations());
                        response.getSqls().addAll(responseChild.getSqls());
                    }
                }
                response.getAggregations().put(parentAgg.getAggName(), Collections.singletonMap(AggBucketsName.BUCKETS.name().toLowerCase(), mapBuckets));
            } else {
                //list类结果
                List<JSONObject> listBuckets = JSONUtils.convert(parentBuckets, List.class);
                for (int i = 0; i < parentAgg.getSubAggs().size(); i++) {
                    Aggregation aggregation = parentAgg.getSubAggs().get(i);
                    for (Map.Entry<Object, String> parentItem : appendWhereSqlForSub.entrySet()) {
                        CkRequest subAggCkRequest = aggResultParserHelper.getSubAggCkRequestByParentAgg(ckRequestContext, parentAgg,
                                aggregation, parentItem.getValue());
                        Response responseChild = executeByAgg(ckRequestContext, aggregation, subAggCkRequest);
                        listBuckets.stream().filter(v -> v.get(AggResultParserHelper.KEY).equals(parentItem.getKey())).findFirst().ifPresent(v -> v.putAll(responseChild.getAggregations()));
                        response.getSqls().addAll(responseChild.getSqls());
                    }
                }
                response.getAggregations().put(parentAgg.getAggName(), Collections.singletonMap(AggBucketsName.BUCKETS.name().toLowerCase(), listBuckets));
            }
        }
    }

    /**
     * 处理sql查询结果，主agg和sug agg一个sql查询。内存处理成最终返回结果。其中，sub agg如果是terms，size限制也在此完成.
     * @param ckRequestContext ckRequestContext
     * @param aggregation      aggregation
     * @param ckRequest        ckRequest
     * @return Response
     * @throws Exception 异常
     */
    public Response executeByAgg(CkRequestContext ckRequestContext, Aggregation aggregation, CkRequest ckRequest) throws Exception {
        Response result = new Response();
        if (ckRequest != null) {
            result = executeByCk(ckRequestContext, aggregation, ckRequest.buildToStr());
        }
        return result;
    }

    public Response buildMSearchAggResult(CkRequestContext ckRequestContext) throws Exception {
        Response result = new Response();
        if (CollectionUtils.isNotEmpty(ckRequestContext.getAggs())) {
            result = executeByAgg(ckRequestContext);
        } else if (ckRequestContext.getSize() == 0) {
            //指标数据，无aggs,size=0，改为需要获取totalCount
            setTotalCount(ckRequestContext, result);
        }
        if (result != null) {
            queryHits(ckRequestContext, result);
            fillCostTime(ckRequestContext, result);
        }
        return result;
    }

    /**
     * 从ck中查询结果.
     * 从ck中查询agg数据。和totalCount，一次查询，内存解析成ui结果。
     * @param ckRequestContext ckRequestContext
     * @param aggregation      aggregation
     * @param aggSql           aggSql
     * @return Response
     * @throws Exception 异常
     */
    private Response executeByCk(CkRequestContext ckRequestContext, Aggregation aggregation, String aggSql) throws Exception {
        Pair<List<JSONObject>, Boolean> aggCkResultAndCacheStatus = ckService.queryDataWithCacheAndStatus(ckRequestContext, aggSql);
        Response result = resultParseService.execute(aggregation, aggCkResultAndCacheStatus.getLeft());
        result.setCache(aggCkResultAndCacheStatus.getRight());
        result.getSqls().add(aggSql);
        return result;
    }

    /**
     * 封装第一个agg,若有并行查询则作peer agg.
     * @param aggsStrategies aggsStrategies
     * @return Aggregation
     */
    private Aggregation buildFirstAgg(List<Aggregation> aggsStrategies) {
        Aggregation result = aggsStrategies.get(0);
        result.setPeerAggs(new ArrayList<>());
        for (int i = 1; i < aggsStrategies.size(); i++) {
            result.getPeerAggs().add(aggsStrategies.get(i));
        }
        return result;
    }
}
