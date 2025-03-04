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
package com.ly.ckibana.handlers;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.compute.aggregation.bucket.TermsBucket;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.exception.FallbackToEsException;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.parser.ParamParser;
import com.ly.ckibana.parser.ResultParser;
import com.ly.ckibana.parser.SearchParser;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.strategy.aggs.Aggregation;
import com.ly.ckibana.util.JSONUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;

@SuppressWarnings("rawtypes")
@Component
public class SearchHandler extends BaseHandler {

    @Resource
    private ParamParser paramParser;

    @Resource
    private CkService ckService;

    @Resource
    private ResultParser resultParseService;
    @Resource
    private SearchParser searchParser;
    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/_search").methods(HttpMethod.GET, HttpMethod.POST),
                HttpRoute.newRoute().path("/{index}/_search").methods(HttpMethod.GET, HttpMethod.POST),
                HttpRoute.newRoute().path("/{index}/{type}/_search").methods(HttpMethod.GET, HttpMethod.POST)
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public String doHandle(RequestContext context) throws Exception {
        String index = context.getIndex();
        // es6.x 查询索引列表时，传入index默认为 * 或 *:*，此时解析请求体，判断是否查询索引列表，如果是的话，就查询ck和es，merge后返回
        if (StringUtils.isEmpty(index)) {
            throw new FallbackToEsException();
        }
        if (!index.equals(Constants.MATCH_ALL) && !context.isCkIndex()) {
            throw new FallbackToEsException();
        }

        IndexPattern indexPattern = paramParser.buildIndexPattern(context);
        List<Aggregation> aggregation = searchParser.parseAggregations(context, indexPattern, index);
        // kibana 创建 index pattern 时，需要查询索引列表，此时需要将ck创建的索引表和es原生索引列表聚合返回给 kibana
        if (isSearchIndexList(aggregation)) {
            return searchIndexList(context, aggregation);
        } else {
            return searchParser.execute(context, index, indexPattern,false);
        }
    }


    /**
     * 查询索引列表
     * kibana 创建 index pattern 时，需要查询索引列表，此时需要将ck创建的索引表和es原生索引列表聚合返回给 kibana
     *
     * @param context
     * @param aggregation
     * @return
     * @throws Exception
     */
    private String searchIndexList(RequestContext context, List<Aggregation> aggregation) throws Exception {
        Response esResponse = JSONObject.parseObject(EsClientUtil.doRequest(context), Response.class);
        Response ckResponse = searchMatchedIndexPattern(context, context.getProxyConfig(), aggregation);
        // merge index list
        Map<String, Map<String, Object>> aggregations = esResponse.getAggregations();
        if (aggregations == null) {
            aggregations = new HashMap<>();
        }
        esResponse.setAggregations(aggregations);

        Map<String, Object> indices = aggregations.getOrDefault("indices", new HashMap<>());
        aggregations.put("indices", indices);

        Object buckets = indices.get("buckets");
        if (buckets == null) {
            buckets = new ArrayList<>();
        }
        indices.put("buckets", buckets);
        Object ck = Optional.of(ckResponse.getAggregations()).map(m -> m.get("indices"))
                .map(m -> m.get("buckets")).orElse(null);

        if (buckets instanceof List esIndices && ck instanceof List ckIndices) {
            esIndices.addAll(ckIndices);
        }
        return JSONUtils.serialize(esResponse);
    }

    /**
     * 是否为indexPattern的查询请求.
     * kibana请求参数demo：{"size":0,"aggs":{"indices":{"terms":{"field":"_index","size":200}}}}
     *
     * @param aggregations 聚合参数
     * @return boolean
     */
    private boolean isSearchIndexList(List<Aggregation> aggregations) {
        return aggregations != null && aggregations.size() == 1 && AggType.TERMS.equals(aggregations.get(0).getAggType()) && Constants.ES_INDEX_QUERY_FIELD.equals(aggregations.get(0).getField());
    }

    /**
     * kibana management-》indexPattern=》查询接口查询匹配到的table.
     *
     * @param context     ck请求上下文
     * @param proxyConfig 代理配置
     * @param aggregation 聚合参数
     * @return Response
     * @throws Exception 异常
     */
    private Response searchMatchedIndexPattern(RequestContext context, ProxyConfig proxyConfig, List<Aggregation> aggregation) throws Exception {
        Response result = new Response();
        List<String> tables;
        if (Constants.INDEX_PATTERN_ALL.contains(context.getIndex()) || StringUtils.isEmpty(context.getIndex())) {
            tables = ckService.queryAllTables(proxyConfig);
        } else {
            tables = ckService.queryTables(proxyConfig, context.getIndex());
        }

        List<TermsBucket> termsBuckets = new ArrayList<>();
        tables.forEach(each -> {
            // ck 索引展示 黑白名单过滤后 的列表，避免后续创建 index pattern 时由于黑白名单过滤后不走 ck，导致创建失败
            if (!proxyConfig.isDirectToEs(each)) {
                TermsBucket termsBucket = new TermsBucket();
                termsBucket.setKey(each);
                termsBuckets.add(termsBucket);
            }
        });
        result.setAggregations(resultParseService.formatResult(aggregation.get(0), Collections.unmodifiableList(termsBuckets), 0));
        return result;
    }
}
