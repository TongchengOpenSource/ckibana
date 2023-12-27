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
package com.ly.ckibana.model.request;

import com.ly.ckibana.strategy.clause.converter.QueryStringClauseConverter;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * queryString查询处理参数实体.
 *
 * @author zl11357
 * @since 2023/10/11 17:44
 */
@Data
@AllArgsConstructor
public class QueryStringClauseParseContext {
    /**
     * 解析到的queryString转换器.
     */
    private List<QueryStringClauseConverter> converters;
    
    /**
     * 前一个字段落.
     */
    private String preParagraph;
    
    /**
     * 当前字段名.
     */

    private String currField;
    
    /**
     * 当前逻辑操作符.
     */

    private String currLogicalOp;
    
    /**
     * 当前段落位置.
     */
    private Integer currentParagraphPos;
    
    /**
     * 查询值的段落位置.
     */
    private Integer valueParagraphsEndPos;
}
