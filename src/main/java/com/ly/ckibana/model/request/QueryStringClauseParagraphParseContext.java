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
public class QueryStringClauseParagraphParseContext {
    
    private String query;
    
    /**
     * 已经解析到的段落列表.
     */
    private List<String> paragraphs;
    
    /**
     * 下一个段落的起始位置.
     */
    private Integer nextParagraphBeginPos;
    
    /**
     * 当前正在解析的字符位置.
     */
    private Integer currentCharPos;
}
