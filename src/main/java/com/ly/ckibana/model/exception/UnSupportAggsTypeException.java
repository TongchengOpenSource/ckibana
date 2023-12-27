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
package com.ly.ckibana.model.exception;

import java.util.List;

public class UnSupportAggsTypeException extends UiException {

    public UnSupportAggsTypeException(String currentValue, List<String> supportValues) {
        super(String.format("不支持当前AggType: %s, 仅支持 %s", currentValue, supportValues.toString()));
    }

    public UnSupportAggsTypeException(String currentValue, String aggField, List<String> supportValues) {
        super(String.format("不支持当前AggType: %s, 当前聚合字段: %s, 仅支持: %s", currentValue, aggField, supportValues.toString()));
    }
}
