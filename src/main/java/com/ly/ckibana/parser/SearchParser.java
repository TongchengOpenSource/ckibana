package com.ly.ckibana.parser;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.exception.UiException;
import com.ly.ckibana.model.property.MetadataConfigProperty;
import com.ly.ckibana.model.property.QueryProperty;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.strategy.aggs.Aggregation;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * search检索解析
 *
 * @Author: zl11357
 * @Email: zl11357@ly.com
 * @Date: 2025/3/4
 */
@Slf4j
@Service
public class SearchParser {

    public static final String RESPONSE = "response";
    @Resource
    private ParamParser paramParser;
    @Resource
    private AggResultParser aggsParser;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Resource
    private MsearchParamParser msearchParamParser;

    @Resource
    private MetadataConfigProperty metadataConfigProperty;

    /**
     * 解析并执行search请求
     *
     * @param context
     * @param index
     * @param indexPattern
     * @return
     * @throws Exception
     */
    public String execute(RequestContext context, String index,
                          IndexPattern indexPattern, boolean asyncSearch) throws Exception {
        CkRequestContext ckRequestContext = new CkRequestContext(context.getClientIp(), indexPattern, paramParser.getMaxResultRow());
        JSONObject searchQuery = JSONUtils.deserialize(context.getRequestInfo().getRequestBody(), JSONObject.class);
        Map<String, Map<String, String>> tableColumnsCache = new HashMap<>();
        String timeField = EsClientUtil.getIndexPatternMeta(context.getProxyConfig().getRestClient(), metadataConfigProperty.getHeaders()).get(index);
        if (timeField == null) {
            log.warn("please set the date field of this index. [{}]", index);
            return JSONUtils.serialize(ProxyUtils.newKibanaException("请设置该索引的date字段"));
        }
        QueryProperty queryProperty = proxyConfigLoader.getKibanaProperty().getQuery();

        try {
            msearchParamParser.parseRequestBySearchQuery(tableColumnsCache, searchQuery, timeField, indexPattern, ckRequestContext);
            if (paramParser.checkIfNeedSampleByIndex(index)) {
                CkRequestContext.SampleParam sampleParam = new CkRequestContext.SampleParam(Constants.USE_SAMPLE_COUNT_THREASHOLD, queryProperty.getSampleCountMaxThreshold());
                long totalCount = aggsParser.queryTotalCount(ckRequestContext, null);
                sampleParam.setSampleTotalCount(totalCount);
                ckRequestContext.setSampleParam(sampleParam);
            }
            Response response = aggsParser.buildMSearchAggResult(ckRequestContext);
            return asyncSearch ? JSONUtils.serialize(Map.of(RESPONSE, response)) : JSONUtils.serialize(response);
        } catch (UiException e) {
            context.getHttpResponse().setStatus(HttpStatus.BAD_REQUEST.value());
            return JSONUtils.serialize(ProxyUtils.newKibanaExceptionV8(e.getUiShow()));
        }
    }


    public List<Aggregation> parseAggregations(RequestContext context, IndexPattern indexPattern, String index) throws Exception {
        JSONObject searchQuery = JSONUtils.deserialize(context.getRequestInfo().getRequestBody(), JSONObject.class);
        CkRequestContext ckRequestContext = new CkRequestContext(context.getClientIp(), indexPattern, paramParser.getMaxResultRow());
        ckRequestContext.setTableName(index);
        ckRequestContext.setColumns(paramParser.queryTableColumns(proxyConfigLoader.getConfig().getCkDatasource(), ckRequestContext.getTableName()));
        return paramParser.parseAggs(Constants.AGG_INIT_DEPTH, ckRequestContext, searchQuery);
    }
}

