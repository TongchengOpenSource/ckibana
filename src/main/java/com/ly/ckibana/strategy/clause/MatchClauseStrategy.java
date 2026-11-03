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

import com.ly.ckibana.model.enums.QueryClauseType;
import org.springframework.stereotype.Component;

/**
 * KQL 字段匹配会生成 match 查询.
 * ClickHouse 没有等价的全文检索，沿用 match_phrase 的模糊匹配近似实现。
 */
@Component
public class MatchClauseStrategy extends MatchPhraseClauseStrategy {

    @Override
    public QueryClauseType getType() {
        return QueryClauseType.MATCH;
    }
}
