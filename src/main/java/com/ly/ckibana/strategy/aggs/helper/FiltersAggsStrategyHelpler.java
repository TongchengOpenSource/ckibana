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
package com.ly.ckibana.strategy.aggs.helper;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.compute.aggregation.AggsParam;
import com.ly.ckibana.model.enums.AggType;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.strategy.clause.ClauseStrategySelector;
import com.ly.ckibana.strategy.aggs.FiltersAggregation;
import com.ly.ckibana.strategy.aggs.FiltersItemAggregation;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * FiltersAggsStrategyHelpler.
 * @author zl11357
 * @since 2023/10/9 16:35
 */
@Component
public class FiltersAggsStrategyHelpler {
    @Resource
    private ClauseStrategySelector clauseStrategySelector;

    public FiltersAggregation generate(AggsParam aggsParam, CkRequestContext ckRequestContext) {
        JSONObject filters = aggsParam.getAggSetting().getJSONObject(AggType.FILTERS.name().toLowerCase());
        List<FiltersItemAggregation> filtersItems = new ArrayList<>();
        filters.keySet().forEach(each -> {
            String filterQuerySql = clauseStrategySelector.buildQuerySql(ckRequestContext, filters.getJSONObject(each));
            filtersItems.add(new FiltersItemAggregation(aggsParam, each, filterQuerySql));
        });
        return new FiltersAggregation(aggsParam, filtersItems);
    }
}
