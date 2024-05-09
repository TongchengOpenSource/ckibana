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
package com.ly.ckibana.handlers;

import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.response.IndexCheckResponse;
import com.ly.ckibana.service.CkService;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class CheckHandler extends BaseHandler {

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Resource
    private CkService ckService;

    @Override
    public List<HttpRoute> routes() {
        return List.of(
                HttpRoute.newRoute().path("/check/{index}").methods(HttpMethod.GET)
        );
    }

    @Override
    public String doHandle(RequestContext context) {
        Map<String, String> urlParams = context.getUrlParams();
        String index = urlParams.get("index");
        IndexCheckResponse response = new IndexCheckResponse();
        try {
            Pair<List<String>, String> tablesWithSql = ckService.queryTablesWithSql(context.getProxyConfig(), index);
            BalancedClickhouseDataSource clickhouseDataSource = context.getProxyConfig().getCkDatasource();
            response.setDatabaseUrls(clickhouseDataSource.getEnabledClickHouseUrls());
            response.setIndex(index);
            response.setDirectToEs(context.getProxyConfig().isDirectToEs(index));
            List<String> whiteList = context.getProxyConfig().getKibanaItemProperty().getWhiteIndexList();
            response.setInWhiteList(whiteList.stream().anyMatch(n -> n.equals(index)));
            response.setHitTables(tablesWithSql.getLeft());
            response.setWhiteList(whiteList);
            response.setSql(tablesWithSql.getRight());
            return JSONUtils.serialize(response);
        } catch (Exception e) {
            log.error("check index error :{}", index, e);
            return ProxyUtils.getErrorResponse(e);
        }
    }
}
