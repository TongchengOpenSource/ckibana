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
package com.ly.ckibana.configure.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ly.ckibana.configure.config.ProxyConfigLoader;
import com.ly.ckibana.model.property.ThreadPoolProperty;
import com.ly.ckibana.service.SqlMonitorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * thread pool manager.
 *
 * @author quzhihao
 */
@Slf4j
@Component
public class ThreadPoolConfigurer implements Closeable {

    private final List<Runnable> shutdownHooks = new ArrayList<>();

    @Getter
    private ThreadPoolExecutor mSearchExecutor;

    @Getter
    private ScheduledThreadPoolExecutor commonScheduledExecutor;

    @Getter
    private ThreadPoolExecutor commonExecutor;

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    @Resource
    private SqlMonitorService sqlMonitorService;

    /**
     * 初始化方法.
     */
    @PostConstruct
    public void init() {
        if (proxyConfigLoader.isUtEnv()) {
            return;
        }
        // init msearch thread pool
        ThreadPoolProperty.ThreadPoolPropertyDetail msearchConfig = proxyConfigLoader.getKibanaProperty().getThreadPool().getMsearchProperty();
        mSearchExecutor = buildFixedExecutor(msearchConfig, "msearch");
        log.info("[thread_pool][msearch] init successful. {}", msearchConfig);

        ThreadPoolProperty.ThreadPoolPropertyDetail commonConfig = proxyConfigLoader.getKibanaProperty().getThreadPool().getCommonProperty();
        commonExecutor = buildFixedExecutor(commonConfig, "common");
        log.info("[thread_pool][common] init successful. {}", msearchConfig);

        // init kibana proxy config thread pool
        ThreadPoolProperty.ThreadPoolPropertyDetail proxyConfig = new ThreadPoolProperty.ThreadPoolPropertyDetail(1, 0);
        commonScheduledExecutor = buildScheduledExecutor(proxyConfig, "proxy");
        log.info("[thread_pool][common] init successful. {}", msearchConfig);

        // init kibana proxy config scheduled task
        commonScheduledExecutor.scheduleAtFixedRate(() -> proxyConfigLoader.refreshConfig(), 10, 10, TimeUnit.SECONDS);
        log.info("[task][refresh config] init successful. {}", msearchConfig);

        commonExecutor.submit(() -> sqlMonitorService.asyncRecordMonitoring());
        log.info("[task][monitoring] init successful. {}", msearchConfig);
    }

    private ThreadPoolExecutor buildFixedExecutor(ThreadPoolProperty.ThreadPoolPropertyDetail configDetail, String name) {
        int coreSize = configDetail.getCoreSize();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(coreSize, coreSize, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(configDetail.getQueueSize()),
                new ThreadFactoryBuilder().setNameFormat(name + "-pool-%d").build());
        shutdownHooks.add(threadPoolExecutor::shutdown);
        return threadPoolExecutor;
    }

    private ScheduledThreadPoolExecutor buildScheduledExecutor(ThreadPoolProperty.ThreadPoolPropertyDetail configDetail, String name) {
        int coreSize = configDetail.getCoreSize();
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(coreSize, new ThreadFactoryBuilder().setNameFormat(name + "-pool-%d").build());
        shutdownHooks.add(scheduledThreadPoolExecutor::shutdown);
        return scheduledThreadPoolExecutor;
    }

    @Override
    public void close() {
        log.info("shutdown thread pool.");
        shutdownHooks.forEach(Runnable::run);
    }
}
