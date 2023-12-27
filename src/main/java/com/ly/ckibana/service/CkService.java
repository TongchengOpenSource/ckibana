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
package com.ly.ckibana.service;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.model.compute.indexpattern.IndexPattern;
import com.ly.ckibana.model.exception.BlackSqlException;
import com.ly.ckibana.model.exception.CKNotSupportException;
import com.ly.ckibana.model.exception.CkSQLException;
import com.ly.ckibana.model.exception.DataSourceEmptyException;
import com.ly.ckibana.model.exception.ResourceExceedException;
import com.ly.ckibana.model.exception.TooManySimultaneousException;
import com.ly.ckibana.model.exception.UnKnowFieldException;
import com.ly.ckibana.model.property.CkProperty;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.except.ClickHouseErrorCode;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ck请求的公用方法.
 */
@Slf4j
@Service
public class CkService {

    public static final int SOCKET_TIMEOUT = 120000;

    public static final int CONNECTION_TIMEOUT = 10000;

    public static final int SLOW_THREAD = 10000;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Resource
    private CkResultCacheService ckResultCacheService;

    @Resource
    private BlackSqlService blackSqlService;

    @Resource
    private SqlMonitorService sqlMonitorService;

    public static String getJdbcUrl(String urlTemplate, String database) {
        return String.format("jdbc:clickhouse://%s/%s", urlTemplate, database);
    }

    public static BalancedClickhouseDataSource initDatasource(CkProperty ckProperty) {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseServerTimeZone(false);
        props.setUseTimeZone("Asia/Shanghai");
        props.setSocketTimeout(SOCKET_TIMEOUT);
        props.setConnectionTimeout(CONNECTION_TIMEOUT);
        props.setUser(ckProperty.getUser());
        props.setPassword(ckProperty.getPass());
        String jdbcUrl = String.format("jdbc:clickhouse://%s/%s", ckProperty.getUrl(), ckProperty.getDefaultCkDatabase());
        return new BalancedClickhouseDataSource(jdbcUrl, props);
    }

    /**
     * 获取jdbc,不同集群获取的地址不同.
     *
     * @param indexPattern 索引模式
     * @return jdbc地址
     */
    public String getJdbcUrl(IndexPattern indexPattern) {
        if (proxyConfigLoader.getKibanaProperty().getProxy().getCk() == null) {
            throw new DataSourceEmptyException("clickhouse数据源为空，请检查配置proxy.ck");
        }
        String ckUrl = proxyConfigLoader.getKibanaProperty().getProxy().getCk().getUrl();
        //基于不同集群，用于读数据的ck
        return getJdbcUrl(ckUrl, indexPattern.getDatabase());
    }

    public ClickHouseConnectionImpl getLimitedConnection(String jdbcUrl) throws SQLException {
        return getConnection(jdbcUrl);
    }

    /**
     * 获取ck连接.
     *
     * @param jdbcUrl jdbc地址
     * @return ck连接
     * @throws SQLException sql异常
     */
    private ClickHouseConnectionImpl getConnection(String jdbcUrl) throws SQLException {
        CkProperty ckProperty = proxyConfigLoader.getKibanaProperty().getProxy().getCk();
        if (ckProperty == null) {
            throw new DataSourceEmptyException("clickhouse数据源为空，请检查配置proxy.ck");
        }
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseServerTimeZone(false);
        props.setUseTimeZone(ZoneId.systemDefault().getId());
        props.setSocketTimeout(SOCKET_TIMEOUT);
        props.setConnectionTimeout(CONNECTION_TIMEOUT);
        props.setUser(ckProperty.getUser());
        props.setPassword(ckProperty.getPass());
        BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource(jdbcUrl, props);
        return (ClickHouseConnectionImpl) dataSource.getConnection();
    }

    public List<String> queryTables(ProxyConfig proxyConfig, String tableName) throws Exception {
        String tableCondition = String.format("name = '%s' ", tableName);
        return queryTablesWithCondition(proxyConfig, tableCondition);
    }

    public List<String> queryAllTables(ProxyConfig proxyConfig) throws Exception {
        return queryTablesWithCondition(proxyConfig, null);
    }

    public List<String> queryTables(ProxyConfig proxyConfig, List<String> tablePrefixes) throws Exception {
        String tableCondition = tablePrefixes.stream().map(name -> String.format("'%s'", name)).collect(Collectors.joining(",", "name in (", ")"));
        return queryTablesWithCondition(proxyConfig, tableCondition);
    }

    private List<String> queryTablesWithCondition(ProxyConfig proxyConfig, String tableCondition) throws Exception {
        String sql = String.format("SELECT name FROM system.tables WHERE database = '%s' ", proxyConfig.getCkDatabase());
        if (StringUtils.isNotEmpty(tableCondition)) {
            sql = String.format("%s AND %s ", sql, tableCondition);
        }
        List<JSONObject> tables = queryData(proxyConfig, sql);
        return tables.stream().map(each -> each.getString("name")).collect(Collectors.toList());
    }

    public Map<String, String> queryColumns(BalancedClickhouseDataSource clickhouseDataSource, String table) throws Exception {
        Map<String, String> result = new HashMap<>();
        String sql = String.format("desc `%s`", table);
        try (ClickHouseConnection connection = clickhouseDataSource.getConnection()) {
            ResultSet results = query(connection, sql);
            ResultSetMetaData metaData = results.getMetaData();
            while (results.next()) {
                Map<String, String> columnMap = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    columnMap.put(metaData.getColumnName(i), results.getString(metaData.getColumnName(i)));
                }
                result.put(columnMap.get("name"), columnMap.get("type"));
            }
            return result;
        }
    }

    private List<JSONObject> queryData(CkRequestContext ckRequestContext, String sql) throws Exception {
        String jdbcUrl = getJdbcUrl(ckRequestContext.getIndexPattern());
        try (ClickHouseConnectionImpl connection = getLimitedConnection(jdbcUrl)) {
            return formatObjectResult(query(connection, sql));
        }
    }

    private List<JSONObject> queryData(ProxyConfig proxyConfig, String sql) throws Exception {
        try (ClickHouseConnection connection = proxyConfig.getCkDatasource().getConnection()) {
            return formatObjectResult(query(connection, sql));
        }
    }

    public Pair<List<JSONObject>, Boolean> queryDataWithCacheAndStatus(CkRequestContext ckRequestContext, String sql) throws Exception {
        Pair<List<JSONObject>, Boolean> resultAndStatus = Pair.of(new ArrayList<>(), false);
        Exception exception = null;

        if (ckRequestContext.getTimeRange() != null && blackSqlService.isBlackSql(ckRequestContext.getTimeRange().getDiffMillSeconds(), ckRequestContext.getQuerySqlWithoutTimeRange())) {
            throw new BlackSqlException(ckRequestContext.getQuerySqlWithoutTimeRange() + "在黑名单中, 禁止执行");
        }
        long startTime = System.currentTimeMillis();

        try {
            if (ckResultCacheService.containsKey(sql)) {
                resultAndStatus = Pair.of(ckResultCacheService.get(sql), true);
            } else {
                List<JSONObject> result = queryData(ckRequestContext, sql);
                ckResultCacheService.put(sql, result);
                resultAndStatus = Pair.of(result, false);
            }
        } catch (Exception e) {
            exception = e;
            log.error("query data error, sql:{}, cost:{}", sql,
                    Duration.between(Instant.ofEpochMilli(startTime), Instant.ofEpochMilli(System.currentTimeMillis())).toMillis(), e);
        }

        long endTime = System.currentTimeMillis();
        try {
            long range = ckRequestContext.getTimeRange() == null ? 0L : ckRequestContext.getTimeRange().getDiffMillSeconds();
            sqlMonitorService.recordAsync(range, ckRequestContext.getQuerySqlWithoutTimeRange(), startTime, endTime);
        } catch (Exception e) {
            log.error("[monitor-add][{}ms] sql={}, startTime={}, endTime={}", ckRequestContext.getQuerySqlWithoutTimeRange(),
                    startTime, endTime,
                    Duration.between(Instant.ofEpochMilli(startTime), Instant.ofEpochMilli(endTime)).toMillis());
        }

        if (exception != null) {
            throw exception;
        }
        return resultAndStatus;
    }

    public List<JSONObject> queryDataWithoutCache(CkRequestContext ckRequestContext, String sql) throws Exception {
        return queryData(ckRequestContext, sql);
    }

    /**
     * 查询ck,拦截不同报错.
     */
    private ResultSet query(ClickHouseConnection connection, String sql) throws Exception {
        try {
            long begin = System.currentTimeMillis();
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(sql);
            long cost = System.currentTimeMillis() - begin;
            if (cost >= SLOW_THREAD) {
                log.warn("[query][slowQuery][{}ms] sql={}", cost, sql);
            } else {
                log.info("[query][{}ms] sql={}", cost, sql);
            }
            return result;
        } catch (ClickHouseException ex) {
            log.error("[query] exception sql: {}", sql, ex);
            if (isResourceExceed(ex.getErrorCode())) {
                throw new ResourceExceedException(ex.getMessage());
            } else if (ex.getErrorCode() == ClickHouseErrorCode.TOO_MUCH_SIMULTANEOUS_QUERIES.code) {
                throw new TooManySimultaneousException(ex.getMessage());
            } else if (ex.getErrorCode() == ClickHouseErrorCode.UNKNOWN_IDENTIFIER.code) {
                String field = ex.getCause().getMessage().split("while processing query")[0];
                field = field.replace("Code: 47, e.displayText() = DB::Exception: Missing columns:", "")
                        .replace("'", "").replace(" ", "");
                throw new UnKnowFieldException(field);
            } else if (ex.getErrorCode() == ClickHouseErrorCode.UNKNOWN_TABLE.code) {
                throw new CKNotSupportException(ex.getMessage());
            } else {
                throw new CkSQLException(ex.getMessage());
            }
        } catch (Exception ex) {
            throw ex;
        }
    }

    /**
     * 检查ck结果行是否超过配置.
     *
     * @param rowCount 行数
     * @throws ResourceExceedException 资源超出异常。
     */
    private void isResultRowCountExceed(Integer rowCount) throws ResourceExceedException {
        if (rowCount >= proxyConfigLoader.getKibanaProperty().getQuery().getMaxResultRow()) {
            throw new ResourceExceedException(String.format("超过代理配置maxResultRow %s", proxyConfigLoader.getKibanaProperty().getQuery().getMaxResultRow()));
        }
    }

    private boolean isResourceExceed(int errorCode) {
        return Arrays.asList(ClickHouseErrorCode.MEMORY_LIMIT_EXCEEDED.code, ClickHouseErrorCode.TOO_MUCH_BYTES.code,
                ClickHouseErrorCode.TOO_MANY_ROWS_OR_BYTES.code, ClickHouseErrorCode.QUOTA_EXPIRED.code).contains(errorCode);
    }

    /**
     * 将ck数据封装为list格式返回.
     */
    private List<JSONObject> formatObjectResult(ResultSet resultSet) throws SQLException, ResourceExceedException {
        List<JSONObject> result = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            JSONObject rowData = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                if (ProxyUtils.isArrayType(metaData.getColumnTypeName(i))) {
                    rowData.put(metaData.getColumnName(i), JSONUtils.convert(resultSet.getArray(i).getArray(), List.class));
                } else {
                    rowData.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
            }
            result.add(rowData);
            isResultRowCountExceed(result.size());
        }
        return result;
    }

}
