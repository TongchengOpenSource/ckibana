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
package com.ly.ckibana.model.response;

import lombok.Data;

/**
 * index pattern field.
 *
 * @author quzhihao
 */
@Data
public class IndexPatternFields {

    private String ckName;

    private String type;

    private Boolean searchable = Boolean.TRUE;

    private Boolean aggregatable = Boolean.TRUE;
    
    private Boolean readFromDocValues = Boolean.TRUE;

    public IndexPatternFields(String ckName, String type) {
        this.ckName = ckName;
        this.type = type;
    }
}
