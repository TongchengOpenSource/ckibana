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
import com.ly.ckibana.model.compute.aggregation.bucket.Bucket;
import com.ly.ckibana.model.compute.aggregation.bucket.BucketStatics;
import com.ly.ckibana.model.compute.aggregation.bucket.BucketsResult;
import com.ly.ckibana.model.compute.aggregation.bucket.FilterInnerBucket;
import com.ly.ckibana.model.compute.aggregation.bucket.MathBucket;
import com.ly.ckibana.model.compute.aggregation.bucket.PercentilesBucket;
import com.ly.ckibana.model.compute.aggregation.bucket.PercentilesRankBucket;
import com.ly.ckibana.model.compute.aggregation.bucket.RangeBucket;
import com.ly.ckibana.model.enums.AggCategory;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.enums.SortType;
import com.ly.ckibana.model.enums.TermsAggOrderType;
import com.ly.ckibana.model.response.Response;
import com.ly.ckibana.strategy.aggs.Aggregation;
import com.ly.ckibana.strategy.aggs.TermsAggStrategy;
import com.ly.ckibana.util.JSONUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 从ck中解析聚合结果，转换为kibana需要的格式返回.
 *
 * @author quzhihao
 */
@Service
public class ResultParser extends HitsResultParser {

    public static final String BUCKET_FIELD_COMPUTE_DATA = "computeData";

    /**
     * 执行聚合请求，将ck数据转换为kibana需要的格式数据.
     *
     * @param aggregation aggregation
     * @param aggCkResult aggCkResult
     * @return Response
     */
    public Response execute(Aggregation aggregation, List<JSONObject> aggCkResult) {
        Response result = new Response();
        List<Bucket> buckets = computeBuckets(aggregation, aggCkResult);
        result.setAggregations(computeAggsResult(aggregation, buckets));
        result.getHits().setTotal(buckets.stream().mapToLong(Bucket::getDocCount).sum());
        return result;
    }

    private Map<String, Map<String, Object>> computeAggsResult(Aggregation aggregation, List<Bucket> buckets) {
        Map<String, Map<String, Object>> peerAggResult = new HashMap<>();
        parseDeepAggData(aggregation, buckets, peerAggResult);
        Map<String, Map<String, Object>> aggsResult = formatResult(aggregation, buckets, 0);
        aggsResult.putAll(peerAggResult);
        return aggsResult;
    }

    /**
     * 将ck结果，经过处理，得到对应的buckets.
     * 1.内存计算主agg doc_count
     * 2.内存计算subAgg size
     * 3.内存计算排序by subAgg
     */
    public List<Bucket> computeBuckets(Aggregation aggregation, List<JSONObject> ckResult) {
        //statics:每一行ck结果解析到的对应各个agg的bucket结果List<Map<聚合staticsId,BucketStatics{Bucket.key(),Bucket}>
        List<Map<String, BucketStatics>> statics = buildCkRowAndAggBucketMappingList(aggregation, ckResult);
        // 解析得到当前agg对应的buckets，包括subBuckets和peerBuckets
        return buildBucketsResult(aggregation, statics).getBuckets();
    }

    private List<Map<String, BucketStatics>> buildCkRowAndAggBucketMappingList(Aggregation aggregation, List<JSONObject> ckResult) {
        List<Map<String, BucketStatics>> result = new ArrayList<>();
        for (int i = 0; i < ckResult.size(); i++) {
            JSONObject obj = ckResult.get(i);
            result.add(buildCkRowAndAggsMapping(aggregation, obj));
        }
        return result;
    }

    /**
     * statics:约定统计数据存放规则（key:每个depth的每个agg,value=bucket,count）.
     */
    private Map<String, BucketStatics> buildCkRowAndAggsMapping(Aggregation aggregation, JSONObject ckResult) {
        Map<String, BucketStatics> result = new HashMap<>();
        if (CollectionUtils.isNotEmpty(aggregation.getSubAggs()) && !aggregation.isIgnoreSubAggCondition()) {
            aggregation.getSubAggs().forEach(each -> result.putAll(buildCkRowAndAggsMapping(each, ckResult)));
        }
        if (CollectionUtils.isNotEmpty(aggregation.getPeerAggs())) {
            aggregation.getPeerAggs().forEach(each -> result.putAll(buildCkRowAndAggsMapping(each, ckResult)));
        }
        result.put(aggregation.getStaticsId(), buildBucketStatics(aggregation, ckResult));
        return result;
    }

    private BucketStatics buildBucketStatics(Aggregation aggregation, JSONObject ckResult) {
        Bucket bucket = aggregation.buildResultBucket(ckResult);
        return new BucketStatics(null == bucket.getKey() ? "unknownKey" : bucket.getKey().toString(), bucket);
    }

    private void parseDeepAggData(Aggregation aggregation, List<Bucket> buckets, Map<String, Map<String, Object>> peerAggDatas) {
        buckets.forEach(bucket -> {
            //注意目前仅第一层有peer agg
            if (CollectionUtils.isNotEmpty(aggregation.getPeerAggs())) {
                aggregation.getPeerAggs().forEach(each -> {
                    BucketsResult bucketsResult = bucket.getComputeData().getPeerBucketMap().get(each.getAggName());
                    List<Bucket> tempBuckets = bucketsResult.getBuckets();
                    parseDeepAggData(each, tempBuckets, peerAggDatas);
                    peerAggDatas.putAll(formatResult(each, tempBuckets, bucketsResult.getOtherDocCount()));
                });
            }
            //任意层可能有child agg
            if (CollectionUtils.isNotEmpty(aggregation.getSubAggs())) {
                Map<String, Map<String, Object>> aggsData = new HashMap<>();
                if (!aggregation.isIgnoreSubAggCondition()) {
                    aggregation.getSubAggs().forEach(each -> {
                        BucketsResult bucketsResult = bucket.getComputeData().getSubBucketMap().get(each.getAggName());
                        List<Bucket> tempBuckets = bucketsResult.getBuckets();
                        parseDeepAggData(each, tempBuckets, peerAggDatas);
                        aggsData.putAll(formatResult(each, tempBuckets, bucketsResult.getOtherDocCount()));
                    });
                    bucket.setComputeSubAggData(aggsData);
                }
            }
        });
    }

    /**
     * 解析得到当前agg对应的buckets.
     * 1.内存计算后，内存处理排序和size截取
     */
    private BucketsResult buildBucketsResult(Aggregation aggsStrategy, List<Map<String, BucketStatics>> bucketStatics) {
        List<Bucket> currentBuckets = buildAggBuckets(aggsStrategy, bucketStatics);
        //内存处理排序和size截取
        List<Bucket> sortedAndSizedBuckets = subSizeBuckets(aggsStrategy, sortBuckets(aggsStrategy, currentBuckets));
        BucketsResult bucketsResult = new BucketsResult(sortedAndSizedBuckets);
        //构建返回结果:sum_other_doc_count,buckets
        bucketsResult.setOtherDocCount(buildOtherDocCountForTermAgg(aggsStrategy, currentBuckets, bucketsResult.getBuckets()));
        return bucketsResult;
    }

    /**
     * 内存截取size.
     * 由于需要整体内存计算后，才能得到排序后数据。因此需要内存处理size
     *
     * @param aggregation    agg
     * @param currentBuckets currentBuckets
     * @return buckets
     */
    private List<Bucket> subSizeBuckets(Aggregation aggregation, List<Bucket> currentBuckets) {
        List<Bucket> result = currentBuckets;
        if (aggregation.getSize() != null && aggregation.getSize() > 0) {
            result = currentBuckets.subList(0, Math.min(currentBuckets.size(), aggregation.getSize()));
        }
        return result;
    }

    /**
     * 为TermAgg补充otherDocCount.
     *
     * @param aggregation    agg
     * @param currentBuckets currentBuckets
     * @param buckets        buckets
     * @return otherDocCount
     */
    private long buildOtherDocCountForTermAgg(Aggregation aggregation, List<Bucket> currentBuckets, List<Bucket> buckets) {
        long otherDocCount = 0;
        if (AggType.TERMS.equals(aggregation.getAggType())) {
            long totalDocCount = currentBuckets.stream().mapToLong(Bucket::getDocCount).sum();
            long showDocCount = buckets.stream().mapToLong(Bucket::getDocCount).sum();
            otherDocCount = totalDocCount - showDocCount;
        }
        return otherDocCount;
    }

    /**
     * 从statics中获取当前agg对应的buckets，包括sub Buckets,peer Buckets.
     *
     * @param aggregation   aggregation
     * @param bucketStatics 所有行数据，包含当前agg/subAgg,peerAgg数据
     * @return buckets
     */
    private List<Bucket> buildAggBuckets(Aggregation aggregation, List<Map<String, BucketStatics>> bucketStatics) {
        String staticsId = aggregation.getStaticsId();
        List<Bucket> result = new ArrayList<>();
        //按照agg的bucketKey的值分类（如DateHistogramAggsStrategy的DateHistogramBucket的Key为时间)。需要LinkedHashMap保持先后顺序
        Map<String, List<Map<String, BucketStatics>>> staticsByAggKeys =
                bucketStatics.stream().collect(Collectors.groupingBy(each -> each.get(staticsId).getBucketKey(), LinkedHashMap::new, Collectors.toList()));
        staticsByAggKeys.forEach((key, value) -> {
            //获取当前agg对应的基本bucket
            Bucket bucket = value.get(0).get(staticsId).getBucket();
            //计算当前bucket对应的总docCount
            buildDocCount(aggregation, value, bucket);
            //内存计算。基于sub agg size计算sub buckets(如items top 10)的结果
            buildSubAggsBuckets(aggregation, value, bucket);
            //针对第一层有兄弟节点
            buildPeerAggsBuckets(aggregation, value, bucket);
            result.add(bucket);
        });
        return result;
    }

    private void buildPeerAggsBuckets(Aggregation aggregation, List<Map<String, BucketStatics>> bucketStatics, Bucket bucket) {
        if (CollectionUtils.isNotEmpty(aggregation.getPeerAggs())) {
            bucket.getComputeData().setPeerBucketMap(new HashMap<>());
            aggregation.getPeerAggs().forEach(each -> {
                bucket.getComputeData().getPeerBucketMap().put(each.getAggName(), buildBucketsResult(each, bucketStatics));
            });
        }
    }

    private void buildSubAggsBuckets(Aggregation aggregation, List<Map<String, BucketStatics>> bucketStatics, Bucket bucket) {
        if (CollectionUtils.isNotEmpty(aggregation.getSubAggs()) && !aggregation.isIgnoreSubAggCondition()) {
            bucket.getComputeData().setSubBucketMap(new HashMap<>());
            aggregation.getSubAggs().forEach(each -> bucket.getComputeData().getSubBucketMap().put(each.getAggName(), buildBucketsResult(each, bucketStatics)));
        }
    }

    /**
     * 计算当前聚合的totalDocCount.
     *
     * @param aggregation   aggregation
     * @param bucketStatics bucketStatics
     * @param bucket        bucket
     */
    private void buildDocCount(Aggregation aggregation, List<Map<String, BucketStatics>> bucketStatics, Bucket bucket) {
        long totalDocCount = 0;
        for (Map<String, BucketStatics> each : bucketStatics) {
            totalDocCount = totalDocCount + each.get(aggregation.getStaticsId()).getBucket().getDocCount();
        }
        bucket.setDocCount(totalDocCount);
    }

    /**
     * 对bucket结果数据增加排序处理-基于内存排序实现.
     * TermsAgg按照avg,max,min,sum等sub agg进行排序，或子查询为 DateHistogramAgg需要内存处理排序
     *
     * @param aggregation aggregation
     * @param buckets     buckets
     * @return buckets
     */
    private List<Bucket> sortBuckets(Aggregation aggregation, List<Bucket> buckets) {
        List<Bucket> result;
        result = sortTermsAggBySubAgg(aggregation, buckets);
        result = sortChildDateHistogramAgg(aggregation, result);
        return result;
    }

    /**
     * TermsAgg按照avg,max,min,sum等sub agg进行排序.
     *
     * @param aggregation aggregation
     * @param buckets     buckets
     * @return buckets
     */
    private List<Bucket> sortTermsAggBySubAgg(Aggregation aggregation, List<Bucket> buckets) {
        List<Bucket> result = buckets;
        if (null == aggregation.getSubAggs() || !AggType.TERMS.equals(aggregation.getAggType())) {
            return result;
        }

        TermsAggStrategy termsAgg = (TermsAggStrategy) aggregation;
        if (TermsAggOrderType.METRIC_CUSTOM.equals(termsAgg.getOrderType())) {
            Aggregation subAgg = termsAgg.getSubAggs().get(termsAgg.getOrderBySubAggIndex());
            if (!TermsAggOrderType.METRIC_CUSTOM.equals(termsAgg.getOrderType()) || !AggCategory.MATH.equals(subAgg.getAggCategory())) {
                return result;
            }
            String orderValue = termsAgg.getOrder().split(" ")[1];
            result = buckets.stream().sorted((o1, o2) -> {
                BucketsResult s1 = o1.getComputeData().getSubBucketMap().get(subAgg.getAggName());
                BucketsResult s2 = o2.getComputeData().getSubBucketMap().get(subAgg.getAggName());
                //math类bucket只有一个bucket
                MathBucket m1 = (MathBucket) s1.getBuckets().get(0);
                MathBucket m2 = (MathBucket) s2.getBuckets().get(0);
                int compareValue = (int) (m1.getValue() - m2.getValue());
                return orderValue.equalsIgnoreCase(SortType.ASC.name()) ? compareValue : (-compareValue);
            }).collect(Collectors.toList());
        }
        return result;
    }

    /**
     * 子查询为 DateHistogramAgg需要内存处理排序.
     *
     * @param aggregation aggregation
     * @param buckets     buckets
     * @return buckets
     */
    private List<Bucket> sortChildDateHistogramAgg(Aggregation aggregation, List<Bucket> buckets) {
        List<Bucket> result = buckets;
        if (AggType.DATE_HISTOGRAM.equals(aggregation.getAggType()) && aggregation.getAggDepth() > Constants.AGG_INIT_DEPTH) {
            result = buckets.stream().sorted((o1, o2) -> {
                Long v1 = Long.parseLong(o1.getKey().toString());
                Long v2 = Long.parseLong(o2.getKey().toString());
                return v1.compareTo(v2);
            }).collect(Collectors.toList());
        }
        return result;
    }

    /**
     * 构建aggregations返回格式.
     *
     * @param aggregation      aggregation
     * @param buckets          buckets
     * @param sumOtherDocCount sumOtherDocCount
     * @return Map
     */
    public Map<String, Map<String, Object>> formatResult(Aggregation aggregation, List<Bucket> buckets, long sumOtherDocCount) {
        AggType aggType = aggregation.getAggType();
        Map<String, Object> bucketsMap = new HashMap<>();
        // avg,max,max,sum 的特殊处理，不是集合，是一个值的map形式
        if (AggCategory.MATH.equals(aggregation.getAggCategory())) {
            if (!buckets.isEmpty()) {
                bucketsMap = buildMatchedCategoryAggsBuckets(aggregation, buckets);
            }
        } else if (AggType.FILTERS_ITEM.equals(aggType)) {
            // 但getSubAggData只有有数据才有值，若buckets为空，需要初始化构造子sub agg
            if (!buckets.isEmpty()) {
                bucketsMap = buildFilterAggsBuckets(aggregation, buckets);
            } else {
                bucketsMap = buildFiltersEmptyResult(aggregation, sumOtherDocCount);
            }
        } else {
            if (aggregation.isKeyed()) {
                //map格式返回
                formatResultByMapType(aggregation, getBuckets(buckets, aggType), bucketsMap);
            } else {
                //常规list buckets返回
                formatResultByListType(aggregation, getBuckets(buckets, aggType), bucketsMap);
            }
        }
        if (AggType.TERMS.equals(aggType)) {
            bucketsMap.put("doc_count_error_upper_bound", 0);
            bucketsMap.put("sum_other_doc_count", sumOtherDocCount);
        }
        Map<String, Map<String, Object>> result = new HashMap<>(1, 1);
        result.put(aggregation.getAggName(), bucketsMap);
        return result;
    }

    /**
     * 获取buckets数据.如包含items类聚合，items为真实用于解析的buckets数据.
     * @param buckets 和聚合对应的外层buckets列表
     * @param aggType 聚合类型
     * @return 返回真实用于解析的bucket列表
     */
    private List<Bucket> getBuckets(List<Bucket> buckets, AggType aggType) {
        if (AggType.RANGE.equals(aggType)) {
            return buckets.isEmpty() ? new ArrayList<>() : Collections.unmodifiableList(((RangeBucket) buckets.get(0)).getItems());
        } else if (AggType.PERCENTILE_RANKS.equals(aggType)) {
            return buckets.isEmpty() ? new ArrayList<>() : Collections.unmodifiableList(((PercentilesRankBucket) buckets.get(0)).getItems());
        } else if (AggType.PERCENTILES.equals(aggType)) {
            return buckets.isEmpty() ? new ArrayList<>() : Collections.unmodifiableList(((PercentilesBucket) buckets.get(0)).getItems());
        } else {
            return buckets;
        }
    }

    private void formatResultByMapType(Aggregation aggregation, List<Bucket> buckets, Map<String, Object> bucketsMap) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        buckets.forEach(each -> {
            Map tempParenBucket = removeComputeVarsFromBucket(each, aggregation.isKeyed());
            if (each.getComputeData().getSubAggData() != null) {
                tempParenBucket.putAll(JSONUtils.convertToMap(each.getComputeData().getSubAggData()));
            }
            map.put(each.getKey().toString(), tempParenBucket);
        });
        bucketsMap.put(aggregation.getAggBucketsName().name().toLowerCase(), map);
    }

    private void formatResultByListType(Aggregation aggregation, List<Bucket> buckets, Map<String, Object> bucketsMap) {
        List<Map> bucketsResult = new ArrayList<>();
        buckets.forEach(each -> {
            Map tempParenBucket = removeComputeVarsFromBucket(each, aggregation.isKeyed());
            if (each.getComputeData().getSubAggData() != null) {
                tempParenBucket.putAll(JSONUtils.convertToMap(each.getComputeData().getSubAggData()));
            }
            bucketsResult.add(tempParenBucket);
        });
        bucketsMap.put(aggregation.getAggBucketsName().name().toLowerCase(), bucketsResult);
    }

    private Map<String, Object> buildFiltersEmptyResult(Aggregation aggStrategy, long sumOtherDocCount) {
        Bucket tempBucKet = new FilterInnerBucket();
        tempBucKet.setKey(aggStrategy.queryFieldName());
        Map<String, Object> result = removeComputeVarsFromBucket(tempBucKet, aggStrategy.isKeyed());
        for (Object each : aggStrategy.getSubAggs()) {
            result.putAll(formatResult((Aggregation) each, new ArrayList<>(), sumOtherDocCount));
        }
        return result;
    }

    /**
     * avg,max,max,sum 的特殊处理，不是集合，是一个值的map形式.
     * filters agg没有自己的bucket,仅用于修改查询条件。buckets直接返回sub agg的。同上面match类型构造
     */
    private Map<String, Object> buildFilterAggsBuckets(Aggregation aggStrategy, List<Bucket> buckets) {
        Map<String, Object> result = removeComputeVarsFromBucket(buckets.get(0), aggStrategy.isKeyed());
        if (buckets.get(0).getComputeData().getSubAggData() != null) {
            result.putAll(JSONUtils.convertToMap(buckets.get(0).getComputeData().getSubAggData()));
        }
        return result;
    }

    private Map<String, Object> buildMatchedCategoryAggsBuckets(Aggregation aggStrategy, List<Bucket> buckets) {
        Map<String, Object> result = removeComputeVarsFromBucket(buckets.get(0), aggStrategy.isKeyed());
        if (buckets.get(0).getComputeData().getSubAggData() != null) {
            result.putAll(JSONUtils.convertToMap(buckets.get(0).getComputeData().getSubAggData()));
        }
        return result;
    }

    /**
     * 转换为具体的实体返回，去掉冗余字段.
     *
     * @param bucket  bucket
     * @param isKeyed isKeyed
     * @return Map
     */
    private Map removeComputeVarsFromBucket(Bucket bucket, Boolean isKeyed) {
        Map result = JSONUtils.convert(bucket, Map.class);
        //avg,max,min,sum等计算类聚合bucket不包括key
        if (bucket.getClass().getSimpleName().equals(MathBucket.class.getSimpleName())) {
            result.remove("key");
            result.remove("doc_count");
        }
        if (isKeyed) {
            //map格式，以key为mapkey
            result.remove("key");
        }
        //删除bucket的内存计算变量
        result.remove(BUCKET_FIELD_COMPUTE_DATA);
        return result;
    }
}
