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
package com.ly.ckibana.strategy.clause;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.compute.QueryClause;
import com.ly.ckibana.model.enums.QueryClauseType;
import com.ly.ckibana.model.request.CkRequestContext;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * ClauseStrategy selector.
 *
 * @author caojiaqiang
 */
@Component
public class ClauseStrategySelector {

    @Resource
    private Collection<ClauseStrategy> handlerList;

    @Getter
    private Map<QueryClauseType, ClauseStrategy> handlerMap;

    /**
     * Initialize all ClauseStrategy.
     */
    @PostConstruct
    public void init() {
        handlerMap = new HashMap<>(handlerList.size());
        if (!CollectionUtils.isEmpty(handlerList)) {
            handlerList.forEach(each -> handlerMap.put(each.getType(), each));
        }
    }

    /**
     * Get strategy by type.
     *
     * @param type strategy type
     * @return ClauseStrategy
     */
    public ClauseStrategy getClauseStrategy(QueryClauseType type) {
        return handlerMap.get(type);
    }

    /**
     * 查询语法clause转换为sql.
     *
     * @param ckRequestContext ckRequestContext
     * @param queryPara           queryPara
     * @return sql
     */
    public String buildQuerySql(CkRequestContext ckRequestContext, JSONObject queryPara) {
        String key = queryPara.keySet().iterator().next();
        String result = "";
        QueryClause clauseDTO = new QueryClause(ckRequestContext, queryPara.getJSONObject(key),
                QueryClauseType.valueOf(key.toUpperCase()));
        ClauseStrategy clauseStrategy = getClauseStrategy(clauseDTO.getType());
        if (clauseStrategy != null) {
            result = clauseStrategy.toSql(clauseDTO);
        }
        return result;
    }
}
