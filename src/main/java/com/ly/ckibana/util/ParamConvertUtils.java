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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 参数转换工具类.
 *
 * @Author: caojiaqiang
 * @createTime: 2023/9/27 5:00 PM
 * @version: 1.0
 * @Description:
 */
@Slf4j
public class ParamConvertUtils {

    /**
     * ui查询中fieldName转换为ckFieldName.
     */
    public static String convertUiFieldToCkField(Map<String, String> columns, String orgField) {
        String result = ProxyUtils.convertUiFieldToEsField(orgField);
        if (!columns.containsKey(result)) {
            result = StringUtils.trim(result);
        }
        // 扩展动态字段查询
        if (!columns.containsKey(result) && columns.containsKey(Constants.CK_EXTENSION)) {
            result = String.format("%s(%s,'%s')", Constants.CK_EXTENSION_QUERY_FUNCTION, Constants.CK_EXTENSION, result);
        }
        return result;
    }
}
