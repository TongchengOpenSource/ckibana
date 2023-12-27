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
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.model.compute.Range;
import com.ly.ckibana.model.enums.SortType;
import com.ly.ckibana.model.request.CkRequest;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.SortedField;
import com.ly.ckibana.model.response.DocValue;
import com.ly.ckibana.model.response.Hit;
import com.ly.ckibana.model.response.HitsOptimizedResult;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.util.DateUtils;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 封装结果类.
 */
@Slf4j
@Service
public class HitsResultParser {

    @Resource
    private CkService ckService;

    /**
     * 查询hit *.
     *
     * @param ckRequestContext ckRequestContext
     * @param response         response
     * @throws Exception Exception
     */
    public void queryHits(CkRequestContext ckRequestContext, Response response) throws Exception {
        if (ckRequestContext.getSize() > 0) {
            CkRequest hitsRequest = buildHitRequest(ckRequestContext);
            optimizeSearchTimeRange(hitsRequest, ckRequestContext, response);
            String optimizedSql = hitsRequest.buildToStr();
            queryHitsFromCk(ckRequestContext, response, optimizedSql);
            response.getSqls().add(String.format("%s", optimizedSql));
        }
    }

    /**
     * 查询count.
     *
     * @param ckRequestContext ckRequestContext
     * @param response         response
     * @return long count
     * @throws Exception Exception
     */
    public long queryTotalCount(CkRequestContext ckRequestContext, Response response) throws Exception {
        long result = 0;
        String sql = getTotalCountQuerySql(ckRequestContext);

        Pair<List<JSONObject>, Boolean> countResultAndStatus = ckService.queryDataWithCacheAndStatus(ckRequestContext, sql);
        if (!countResultAndStatus.getLeft().isEmpty()) {
            result = countResultAndStatus.getLeft().get(0).getLongValue(SqlConstants.DEFAULT_COUNT_NAME);
        }
        if (response == null) {
            return result;
        }
        response.setCache(countResultAndStatus.getRight());
        response.getSqls().add(sql);
        return result;
    }

    /**
     * 构建查询总条数的sql.
     *
     * @param ckRequestContext ckRequestContext
     * @return sql
     */
    public String getTotalCountQuerySql(CkRequestContext ckRequestContext) {
        CkRequest countRequest = ProxyUtils.buildCkRequest(ckRequestContext);
        countRequest.initSelect(SqlConstants.COUNT_QUERY);
        if (StringUtils.isNotBlank(ckRequestContext.getQuery())) {
            countRequest.appendWhere(ckRequestContext.getQuery());
        }
        return countRequest.buildToStr();
    }

    /**
     * 从ck中查询hits，处理后返回.
     *
     * @param ckRequestContext ckRequestContext
     * @param response         response
     * @param optimizedSql     optimizedSql
     * @throws Exception Exception
     */
    private void queryHitsFromCk(CkRequestContext ckRequestContext, Response response, String optimizedSql) throws Exception {
        Pair<List<JSONObject>, Boolean> hitResultAndStatus = ckService.queryDataWithCacheAndStatus(ckRequestContext, optimizedSql);
        response.getHits().setHits(buildHitsResponse(ckRequestContext, hitResultAndStatus.getLeft()));
        response.setCache(hitResultAndStatus.getRight());
    }

    /**
     * 基于核心业务逻辑条件，获取每分钟的count统计值.
     * 用于优化带时间排序查询明细下的区间。如用户查询1小时数据的最近10条，加入最后1分钟数据>10条。实际可用最后1分钟作为时间条件查询明细
     *
     * @param ckRequestContext ckRequestContext
     * @param optimizedContext optimizedContext
     * @param orgTimeRange     orgTimeRange
     * @return List count
     * @throws Exception Exception
     */
    private List<JSONObject> queryCountByDivMinutes(CkRequestContext ckRequestContext, HitsOptimizedResult optimizedContext,
                                                    Range orgTimeRange) throws Exception {
        CkRequest ckRequest = getByMinuteRequest(optimizedContext.getSortType(), ckRequestContext, orgTimeRange);
        String sql = orgTimeRange.toSql(true);
        ckRequest.appendWhere(sql);
        ckRequest.appendWhere(ckRequestContext.getQuerySqlWithoutTimeRange());

        String countByMinutesQuerySql = ckRequest.buildToStr();
        optimizedContext.setCountByMinutesQuerySql(countByMinutesQuerySql);
        Pair<List<JSONObject>, Boolean> resultAndStatus = ckService.queryDataWithCacheAndStatus(ckRequestContext, countByMinutesQuerySql);
        optimizedContext.setCache(resultAndStatus.getRight());
        return resultAndStatus.getLeft();
    }

    public CkRequest buildHitRequest(CkRequestContext ckRequestContext) {
        CkRequest result = ProxyUtils.buildCkRequest(ckRequestContext);
        result.limit(ckRequestContext.getSize());
        result.orderBy(ckRequestContext.getSort());
        result.appendWhere(ckRequestContext.getQuery());
        return result;
    }

    /**
     * 封装hits结构.
     * 展开extention字段，
     * 字段名转换为es字段名
     * 封装sort,fields字段
     *
     * @param ckRequestContext ckRequestContext
     * @param hitResult        hitResult
     * @return List hits结果
     */
    private List<Hit> buildHitsResponse(CkRequestContext ckRequestContext, List<JSONObject> hitResult) {
        List<Hit> result = new ArrayList<>();
        for (int i = 0; i < hitResult.size(); i++) {
            Hit hitTemp = new Hit();
            //解压扩展字段,ck field->es field
            JSONObject hitSource = setHitResource(hitResult, i, hitTemp);
            hitTemp.setIndex(generateHitsIndexValue(ckRequestContext.getTableName(), hitSource.get(ckRequestContext.getIndexPattern().getTimeField())));
            hitTemp.setId(generateHitsId(i, hitTemp));
            setHitFieldsResponse(ckRequestContext, hitSource, hitTemp);
            setHitSortResponse(ckRequestContext, hitSource, hitTemp);
            result.add(hitTemp);
            //total已经在优化中设置
        }
        return result;
    }

    private String generateHitsId(int index, Hit hitTemp) {
        return hitTemp.getIndex() + "_t" + System.currentTimeMillis() + "_i" + index;
    }

    /**
     * 生成hits index_id，非核心功能，异常返回空.
     *
     * @param tableName tableName
     * @param timeValue timeValue
     * @return String index_id
     */
    private String generateHitsIndexValue(String tableName, Object timeValue) {
        try {
            return String.format("%s-%s", tableName, DateUtils.getDateStr(timeValue));
        } catch (Exception e) {
            log.error("generateHitsIndexValue error，tableName:{}, timeValue:{}", tableName, timeValue, e);
            return StringUtils.EMPTY;
        }
    }

    /**
     * 仅按照时间排序采用优化策略.
     * 仅支持按时间排序的请求使用优化
     * 计算1分钟内的数量（若参数fromTime=2021-03-16 15:01:01.111）,则endTime=2021-03-16 15:01:59.999.若大于size,则maxTime=endTime
     * 若不满足查询size,按照分钟切割，看每分钟的count数据。beginTime=2021-03-16 15:02:00.000 查询beginTime-toTime，按分钟切割，若某一分钟时数量大于size,则toTime修改为当前time
     *
     * @param hitRequest       hitRequest
     * @param ckRequestContext ckRequestContext
     * @param response         response
     * @throws Exception Exception
     */
    private void optimizeSearchTimeRange(CkRequest hitRequest, CkRequestContext ckRequestContext, Response response) throws Exception {
        HitsOptimizedResult hitsOptimizedResult = new HitsOptimizedResult();
        if (isOptimizeAble(ckRequestContext)) {
            hitsOptimizedResult.setSortType(ckRequestContext.getSortingFields().get(0).getType());
            HitsOptimizedResult optimizedResult = doOptimizeSearchTimeRange(ckRequestContext, hitsOptimizedResult);
            setHitsTotalCountByOptimizedResult(response, optimizedResult.getTotalCount());
            response.getSqls().add(hitsOptimizedResult.getCountByMinutesQuerySql());
        }
        // 若被优化，则将优化后的时间+业务条件拼接位最终查询条件
        if (hitsOptimizedResult.isOptimized()) {
            hitRequest.resetWhere();
            hitRequest.appendWhere(ckRequestContext.getQuerySqlWithoutTimeRange());
            hitRequest.appendWhere(hitsOptimizedResult.getOptimizedTimeRange().toSql(true));
        }
    }

    /**
     * 是否查询时间区间可被优化.
     * a)有时间区间参数
     * b)仅基于时间排序
     *
     * @param ckRequestContext ckRequestContext
     * @return boolean 是否可被优化
     */
    private boolean isOptimizeAble(CkRequestContext ckRequestContext) {
        return ckRequestContext.getTimeRange() != null && ckRequestContext.getSortingFields().size() == 1
                && ckRequestContext.getSortingFields().get(0).getCkFieldName().equals(ckRequestContext.getIndexPattern().getTimeField());
    }

    /**
     * 按分钟粒度切割，目标是缩短实际查询时间.
     * 同时计算totalCount
     *
     * @param ckRequestContext ckRequestContext
     * @param optimizedContext optimizedContext
     * @return HitsOptimizedResult HitsOptimizedResult
     * @throws Exception Exception
     */
    private HitsOptimizedResult doOptimizeSearchTimeRange(CkRequestContext ckRequestContext,
                                                          HitsOptimizedResult optimizedContext) throws Exception {
        Range orgTimeRange = ckRequestContext.getTimeRange();
        long requestSize = ckRequestContext.getSize();
        Range optimizedTimeRange = JSONUtils.copy(orgTimeRange);
        long totalCount = 0L;
        //查询每分钟统计count
        List<JSONObject> byMinuteResult = queryCountByDivMinutes(ckRequestContext, optimizedContext, orgTimeRange);
        boolean isFindThreshOld = false;
        for (JSONObject each : byMinuteResult) {
            totalCount = totalCount + each.getLongValue(SqlConstants.DEFAULT_COUNT_NAME);
            if (!isFindThreshOld && totalCount >= requestSize) {
                long realTime = each.getLongValue(SqlConstants.CK_MINUTE_NAME) * 1000 * 60;
                if (SortType.ASC.equals(optimizedContext.getSortType())) {
                    //升序是 是这一分钟的59.999 结束
                    optimizedTimeRange.setHigh(realTime + 59999);
                    optimizedTimeRange.setLessThanEq(true);
                } else {
                    //降序是 是这一分钟的00.000开始
                    optimizedTimeRange.setLow(realTime);
                    optimizedTimeRange.setMoreThanEq(true);
                }
                isFindThreshOld = true;
            }
        }
        optimizedContext.setOptimized(isFindThreshOld);
        optimizedContext.setTotalCount(totalCount);
        optimizedContext.setOptimizedTimeRange(optimizedTimeRange);
        return optimizedContext;
    }

    /**
     * 基于核心业务逻辑条件，构建获取每分钟的count的CkRequest.
     *
     * @param sortType         排序类型
     * @param ckRequestContext ckRequestContext
     * @param orgTimeClause    orgTimeClause
     * @return CkRequest CkRequest
     */
    private CkRequest getByMinuteRequest(SortType sortType, CkRequestContext ckRequestContext, Range orgTimeClause) {
        CkRequest byMinuteRequest = ProxyUtils.buildRequest(ckRequestContext.getTableName(), SqlConstants.COUNT_QUERY);
        String timeField = ProxyUtils.generateTimeFieldSqlWithFormatUnixTimestamp64Milli(ckRequestContext.getIndexPattern().getTimeField(), orgTimeClause.getCkFieldType());
        byMinuteRequest.appendSelect(String.format(SqlConstants.TIME_AGG_BY_MINUTE_TEMPLATE, timeField, SqlConstants.CK_MINUTE_NAME));
        byMinuteRequest.initGroupBy(SqlConstants.CK_MINUTE_NAME);
        byMinuteRequest.orderBy(SqlConstants.CK_MINUTE_NAME + " " + sortType.name());
        return byMinuteRequest;
    }

    private void setHitsTotalCountByOptimizedResult(Response response, Long tempCount) {
        //设置hit总数
        response.getHits().setTotal(tempCount);
    }

    private JSONObject setHitResource(List<JSONObject> hitResult, int index, Hit hitTemp) {
        JSONObject result = hitResult.get(index);
        unZipExtension(result);
        hitTemp.setSource(result);
        return result;
    }

    /**
     * 设置hits fields table展示需要.
     *
     * @param ckRequestContext ckRequestContext
     * @param hit              hit
     * @param hitTemp          hitTemp
     */
    private void setHitFieldsResponse(CkRequestContext ckRequestContext, JSONObject hit, Hit hitTemp) {
        Map<String, Object> fields = new HashMap<>();
        List<DocValue> docValues = ckRequestContext.getDocValues();
        for (DocValue each : docValues) {
            String uiField = each.getFieldName();
            if (!hit.containsKey(uiField)) {
                continue;
            }
            fields.put(uiField, List.of(hit.get(uiField)));
        }
        hitTemp.setFields(fields);
    }

    /**
     * 设置hits sort table展示需要.
     *
     * @param ckRequestContext ckRequestContext
     * @param v                v
     * @param hitTemp          hitTemp
     */
    private void setHitSortResponse(CkRequestContext ckRequestContext, JSONObject v, Hit hitTemp) {
        List<Object> sort = new ArrayList<>();
        List<SortedField> sortedFields = ckRequestContext.getSortingFields();
        for (SortedField each : sortedFields) {
            String tempCkSortFieldName = each.getOrgFiledName();
            sort.add(v.get(tempCkSortFieldName));
        }
        hitTemp.setSort(sort);
    }

    /**
     * 总数.
     *
     * @param ckRequestContext ckRequestContext
     * @param response         response
     * @throws Exception Exception
     */
    public void setTotalCount(CkRequestContext ckRequestContext, Response response) throws Exception {
        long totalCount = queryTotalCount(ckRequestContext, response);
        response.getHits().setTotal(totalCount);
    }

    private void unZipExtension(JSONObject obj) {
        String json = obj.getString(Constants.CK_EXTENSION);
        if (StringUtils.isNotBlank(json)) {
            JSONObject extension = JSONUtils.deserialize(json, JSONObject.class);
            if (extension != null && !extension.keySet().isEmpty()) {
                extension.keySet().forEach(each -> obj.put(each, extension.get(each)));
            }
        }
        obj.remove(Constants.CK_EXTENSION);
    }

    /**
     * 设置操作耗时和默认status.
     *
     * @param ckRequestContext ckRequestContext
     * @param response         response
     */
    public void fillCostTime(CkRequestContext ckRequestContext, Response response) {
        response.setTook(((Long) (System.currentTimeMillis() - ckRequestContext.getBeginTime())).intValue());
        response.setStatus(HttpStatus.OK.value());
    }
}
