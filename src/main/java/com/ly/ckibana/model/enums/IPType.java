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
package com.ly.ckibana.model.enums;

import com.ly.ckibana.constants.SqlConstants;
import lombok.Getter;

/**
 * IP地址类型.
 *
 * @author zl11357
 * @since 2023/10/12 16:50
 */
public enum IPType {
    IPV4_STRING(SqlConstants.TYPE_STRING, Boolean.FALSE, "IPv4StringToNumOrDefault", "IPv4StringToNum"),
    IPV6_STRING(SqlConstants.TYPE_STRING, Boolean.FALSE, "IPv6StringToNumOrDefault", "IPv6StringToNum"),
    IPV4("IPv4", Boolean.TRUE, "", "IPv4StringToNumOrDefault"),
    IPV6("IPv6", Boolean.TRUE, "", "IPv6StringToNumOrDefault");

    @Getter
    private String ckType;

    @Getter
    private Boolean isCkIpType;

    @Getter
    private String ckFiledFunction;

    @Getter
    private String ckValueFunction;

    IPType(String ckType, Boolean isCkIpType, String ckFiledFunction, String ckValueFunction) {
        this.ckType = ckType;
        this.isCkIpType = isCkIpType;
        this.ckFiledFunction = ckFiledFunction;
        this.ckValueFunction = ckValueFunction;
    }

    public String getCkType() {
        return ckType;
    }
    
    public String getCkFiledFunction() {
        return ckFiledFunction;
    }

    public String getCkValueFunction() {
        return ckValueFunction;
    }
}
