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
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.property.MetadataConfigProperty;
import com.ly.ckibana.util.RestUtils;
import com.ly.ckibana.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 将sqlTemplate 存储到es里面 留着分析使用.
 */
@Slf4j
@Service
public class SqlMonitorService {

    private static final int SEND_BATCH_SIZE = 500;

    private static final int SEND_PERIOD_SECOND = 5;

    private RestClient restClient;

    @Resource
    private MetadataConfigProperty metadataConfigProperty;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    private LinkedBlockingQueue<Map<String, Object>> dataQueue;

    @PostConstruct
    public void init() {
        if (proxyConfigLoader.isUtEnv()) {
            return;
        }
        restClient = RestUtils.createRequestContext(metadataConfigProperty.getHosts(), metadataConfigProperty.getHeaders()).getProxyConfig().getRestClient();
        dataQueue = new LinkedBlockingQueue<>(proxyConfigLoader.getKibanaProperty().getThreadPool().getCommonProperty().getQueueSize());
    }

    public void recordAsync(long range, String sql, long startTime, long endTime) {
        boolean result = dataQueue.offer(buildBulkBody(range, sql, startTime, endTime));
        if (!result) {
            log.warn("monitor queue is full. current size:{}", dataQueue.size());
        }
    }

    public void batchRecord(List<Map<String, Object>> dataList) {
        long monitorStartTime = System.currentTimeMillis();
        boolean enableMonitoring = proxyConfigLoader.getKibanaProperty().getProxy().isEnableMonitoring();
        if (!enableMonitoring) {
            return;
        }
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }

        String dateTimeIndexName = Utils.getIndexName(Constants.ConfigFile.MONITOR_INDEX_NAME, Constants.DATE_FORMAT_DEFAULT);
        EsClientUtil.saveBatch(restClient, dateTimeIndexName, dataList, proxyConfigLoader.getMajorVersion());
        long cost = Duration.between(Instant.ofEpochMilli(monitorStartTime), Instant.ofEpochMilli(System.currentTimeMillis())).toMillis();
        log.info("[monitor-add][{}ms] index=[{}], {}", cost, dateTimeIndexName, JSONObject.toJSONString(dataList));
    }

    private Map<String, Object> buildBulkBody(long range, String key, long startTime, long endTime) {
        Map<String, Object> map = new HashMap<>(2);
        map.put("key", key);
        map.put("range", range);
        map.put("startTime", startTime);
        map.put("endTime", endTime);
        map.put("cost", Duration.between(Instant.ofEpochMilli(startTime), Instant.ofEpochMilli(endTime)).toMillis());
        map.put("timestamp", System.currentTimeMillis());
        return map;
    }

    public void asyncRecordMonitoring() {
        if (proxyConfigLoader.isUtEnv()) {
            return;
        }
        List<Map<String, Object>> dataList = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        while (true) {
            try {
                Map<String, Object> data = dataQueue.poll(1, TimeUnit.SECONDS);
                if (data != null) {
                    dataList.add(data);
                }
                boolean send = dataList.size() >= SEND_BATCH_SIZE || System.currentTimeMillis() - currentTime > SEND_PERIOD_SECOND * 1000;
                if (!send) {
                    continue;
                }
                batchRecord(dataList);
                dataList.clear();
                TimeUnit.SECONDS.sleep(SEND_PERIOD_SECOND);
                currentTime = System.currentTimeMillis();
            } catch (Exception e) {
                log.error("send monitor data to es error", e);
                try {
                    TimeUnit.SECONDS.sleep(SEND_PERIOD_SECOND);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
