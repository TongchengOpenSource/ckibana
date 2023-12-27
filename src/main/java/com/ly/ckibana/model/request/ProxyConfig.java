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

import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.exception.DataSourceEmptyException;
import com.ly.ckibana.model.property.CkProperty;
import com.ly.ckibana.model.property.KibanaItemProperty;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.util.EsProxyClientConsumer;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.RestUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.util.Map;
import java.util.Objects;

@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class ProxyConfig {
    public static final Logger log = LoggerFactory.getLogger(ProxyConfig.class);

    private KibanaItemProperty kibanaItemProperty;

    private RestClient restClient;

    private EsProxyClientConsumer esClientBuffer;

    private BalancedClickhouseDataSource ckDatasource;

    public ProxyConfig(KibanaItemProperty kibanaItemProperty) {
        this.kibanaItemProperty = kibanaItemProperty;
        this.restClient = RestUtils.initEsResClient(kibanaItemProperty.getEs());
        this.esClientBuffer = new EsProxyClientConsumer();
        if (kibanaItemProperty.getCk() != null) {
            this.ckDatasource = CkService.initDatasource(kibanaItemProperty.getCk());
        } else {
            this.ckDatasource = null;
        }
    }

    public String getCkDatabase() {
        if (kibanaItemProperty.getCk() == null) {
            throw new DataSourceEmptyException("clickhouse数据源为空，请检查配置proxy.ck");
        }
        return kibanaItemProperty.getCk().getDefaultCkDatabase();
    }

    public boolean shouldUpdateEsClient(KibanaItemProperty properties) {
        if (properties == null || properties.getEs() == null) {
            return false;
        }
        if (StringUtils.isEmpty(this.kibanaItemProperty.getEs().getHost()) && StringUtils.isEmpty(properties.getEs().getHost())) {
            return false;
        }
        boolean hostDiff = !Objects.equals(this.kibanaItemProperty.getEs().getHost(), properties.getEs().getHost());
        Map<String, String> oldHeaders = this.kibanaItemProperty.getEs().getHeaders();
        Map<String, String> newHeaders = properties.getEs().getHeaders();
        boolean headersDiff;
        if (oldHeaders != null && newHeaders != null) {
            headersDiff = !oldHeaders.toString().equals(newHeaders.toString());
        } else {
            headersDiff = !(oldHeaders == null && newHeaders == null);
        }
        return headersDiff || hostDiff;
    }

    public boolean shouldUpdateCkClient(KibanaItemProperty properties) {
        if (properties == null || properties.getCk() == null) {
            return false;
        }
        CkProperty oldProperty = this.kibanaItemProperty.getCk();
        CkProperty newProperty = properties.getCk();
        if (oldProperty == null && newProperty != null) {
            return true;
        }
        return !Objects.equals(newProperty, oldProperty);
    }

    public boolean isDirectToEs(String index) {
        if (StringUtils.isEmpty(index)) {
            return true;
        }
        if (CollectionUtils.isEmpty(kibanaItemProperty.getBlackIndexList()) && CollectionUtils.isEmpty(kibanaItemProperty.getWhiteIndexList())) {
            return true;
        }
        if (!CollectionUtils.isEmpty(kibanaItemProperty.getBlackIndexList())) {
            for (String each : kibanaItemProperty.getBlackIndexList()) {
                if (index.equals(each)) {
                    return true;
                }
            }
        }
        if (!CollectionUtils.isEmpty(kibanaItemProperty.getWhiteIndexList())) {
            for (String each : kibanaItemProperty.getWhiteIndexList()) {
                if (index.equals(each)) {
                    return false;
                }
            }
        }
        return true;
    }

    public IndexPattern buildIndexPattern(String originIndex) {
        String index = ProxyUtils.trimRemoteCluster(originIndex);
        return buildIndexPattern(index, index);
    }

    public IndexPattern buildIndexPattern(String uiIndex, String index) {
        IndexPattern indexPattern = new IndexPattern();
        String database = getCkDatabase();
        indexPattern.setUiIndex(uiIndex);
        indexPattern.setIndex(index);
        indexPattern.setDatabase(database);
        return indexPattern;
    }
}
