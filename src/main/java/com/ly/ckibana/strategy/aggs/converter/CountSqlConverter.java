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
package com.ly.ckibana.strategy.aggs.converter;

import com.google.common.base.Strings;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.util.SqlUtils;
import com.ly.ckibana.util.Utils;
import lombok.Data;

@Data
public class CountSqlConverter implements SqlConverter {

    private String name;

    private String condition;

    private String functionCondition;

    @Override
    public String toSql(double sample) {
        String sampleMathStr = "";
        if (!Strings.isNullOrEmpty(functionCondition)) {
            return SqlUtils.getCountString(functionCondition, sampleMathStr, name);
        }
        // 采样
        if (sample < Constants.DEFAULT_NO_SAMPLE) {
            sampleMathStr = "/" + Utils.double2Scale(sample);
        }
        if (!Strings.isNullOrEmpty(condition)) {
            return SqlUtils.getCountIfString(condition, sampleMathStr, name);
        }
        return SqlUtils.getCountString(1, sampleMathStr, name);
    }
}
