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
package com.ly.ckibana.model.property;

import com.ly.ckibana.constants.Constants;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Data
public class KibanaItemProperty {

    private CkProperty ck;

    private EsProperty es;

    /**
     * 超过ROUND_ABLE_MIN_PERIOD 支持round，单位ms.
     */
    private long roundAbleMinPeriod = 120000;

    /**
     * round值，单位s.
     */
    private int round;

    /**
     * 支持的时间周期.
     */
    private long maxTimeRange;

    /**
     * 默认时间字段
     */
    private String defaultTimeFieldName= Constants.DEFAULT_TIME_FILED_NAME;

    private List<String> blackIndexList = new ArrayList<>();

    private List<String> whiteIndexList = new ArrayList<>();

    private boolean enableMonitoring;

    public KibanaItemProperty() {
    }

    public KibanaItemProperty(String hosts, Map<String, String> headers) {
        this.es = new EsProperty(hosts, headers);
        this.blackIndexList = new ArrayList<>();
        this.whiteIndexList = new ArrayList<>();
        this.enableMonitoring = false;
        this.ck = null;
    }

    public static KibanaItemProperty buildProxy(KibanaItemProperty updateProxy, Supplier<KibanaItemProperty> init) {
        KibanaItemProperty defaultProxyProperty = init.get();
        if (updateProxy == null) {
            return defaultProxyProperty;
        } else {
            if (updateProxy.getEs() == null) {
                updateProxy.setEs(defaultProxyProperty.getEs());
            }
            return updateProxy;
        }
    }
}
