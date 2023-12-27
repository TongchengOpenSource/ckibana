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
package com.ly.ckibana.util;

import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.SqlConstants;
import org.apache.commons.lang3.StringUtils;

/**
 * 查询转换工具类.
 *
 * @Author: caojiaqiang
 * @createTime: 2023/9/27 5:00 PM
 * @version: 1.0
 * @Description:
 */
public class QueryConvertUtils {

    /**
     * 解析queryString语法.
     * caseIgnore，equal,like
     *
     * @param uiField     ui字段
     * @param ckFieldName ck字段名
     * @param body        转换body
     * @param type        ck字段类型
     * @return sql
     */
    public static String convertMatchPhraseToSql(String uiField, String ckFieldName, Object body, String type) {
        String result;
        // 忽略大小写查询语法
        if (ProxyUtils.isCaseIgnoreQuery(uiField) || ProxyUtils.isCaseIgnoreKeywordStringQuery(uiField)) {
            result = convertCaseIgnoreQueryToSql(uiField, ckFieldName, body);
        } else if (ProxyUtils.isPreciseField(uiField)) {
            // host或带.keyword精确查询
            result = SqlUtils.getEqualsSql(ckFieldName, body, Boolean.TRUE);
        } else {
            result = SqlUtils.getLikeByType(ckFieldName, (String) ProxyUtils.trimNull(body), type);
        }
        return result;
    }

    private static String convertCaseIgnoreQueryToSql(String field, String ckFieldName, Object body) {
        String result;
        if (ProxyUtils.isCaseIgnoreKeywordStringQuery(field)) {
            StringUtils.replace(field, Constants.UI_PHRASE_CASE_IGNORE + Constants.ES_KEYWORD, StringUtils.EMPTY);
            //忽略大小写精确查询
            result = SqlUtils.getEqualsSql(SqlUtils.getLowerString(ProxyUtils.getPureColumnName(field)), body.toString().toLowerCase(), Boolean.TRUE);
        } else {
            //忽略大小写模糊查询
            result = String.format(SqlConstants.FUNCTION_FOR_CASE_INSENSITIVE, ckFieldName, body);
        }
        return result;
    }
}
