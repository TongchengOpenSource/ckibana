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
package com.ly.ckibana.util;

import com.ly.ckibana.constants.SecurityProtocolEnum;
import com.ly.ckibana.model.property.EsProperty;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.util.CollectionUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.http.HttpServletRequest;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RestUtils {

    private static final String[] HEADERS_TO_TRY = {
            "real-ip-new",
            "x-forwarded-for",
            "x-forwarded-host",
            "Proxy-Client-IP",
            "WL-Proxy-Client-IP",
            "HTTP_X_FORWARDED_FOR",
            "HTTP_X_FORWARDED",
            "HTTP_X_CLUSTER_CLIENT_IP",
            "HTTP_CLIENT_IP",
            "HTTP_FORWARDED_FOR",
            "HTTP_FORWARDED",
            "HTTP_VIA",
            "REMOTE_ADDR"};

    /**
     * 获取客户端ip.
     */
    public static String getClientIpAddr(HttpServletRequest request) {
        for (String each : HEADERS_TO_TRY) {
            String ip = request.getHeader(each);
            if (ip != null && ip.length() != 0 && !"unknown".equalsIgnoreCase(ip)) {
                return ip;
            }
        }
        return request.getRemoteAddr();
    }

    /**
     * 初始化es客户端.
     */
    public static RestClient initEsResClient(EsProperty item, boolean mergeDefaultHeader) {
        Map<String, String> headersMap = new HashMap<>();
        if (!CollectionUtils.isEmpty(item.getHeaders()) && mergeDefaultHeader) {
            headersMap.putAll(item.getHeaders());
        }
        return initEsRestClient(item.getHost(), headersMap);
    }

    public static RestClient initEsRestClient(String host, Map<String, String> headersMap) {
        try {
            String[] hostSplit = host.split(",");
            HttpHost[] hosts = new HttpHost[hostSplit.length];
            for (int i = 0; i < hostSplit.length; i++) {
                String each = hostSplit[i];
                String scheme = HttpHost.DEFAULT_SCHEME_NAME;
                if (each.matches("^[^:]+://.*")) {
                    int endIndex = each.indexOf("://");
                    scheme = each.substring(0, endIndex);
                    each = each.substring(endIndex + 3);
                }

                if (each.contains(":")) {
                    String[] splits = each.split(":");
                    hosts[i] = new HttpHost(splits[0], Integer.parseInt(splits[1]), scheme);
                } else {
                    int port = SecurityProtocolEnum.HTTPS.getScheme().equals(scheme) ? SecurityProtocolEnum.HTTPS.getPort() : SecurityProtocolEnum.HTTP.getPort();
                    hosts[i] = new HttpHost(each, port, scheme);
                }
            }
            headersMap = headersMap == null ? new HashMap<>(0) : headersMap;
            Header[] headers = new Header[headersMap.size()];
            int i = 0;
            for (Map.Entry<String, String> header : headersMap.entrySet()) {
                headers[i] = new BasicHeader(header.getKey(), header.getValue());
                i++;
            }

            RestClientBuilder builder = RestClient.builder(hosts);
            return builder.setHttpClientConfigCallback(new EsHttpConfigCallback())
                    .setDefaultHeaders(headers)
                    .build();
        } catch (Exception ex) {
            log.error("创建客户端失败,host:{}, headersMap:{}", host, headersMap, ex);
            throw ex;
        }
    }

    public static RequestContext createRequestContext(String urls, Map<String, String> headers) {
        RequestContext requestContext = new RequestContext();
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setRestClient(RestUtils.initEsRestClient(
                        urls,
                        headers
                )
        );
        proxyConfig.setEsClientBuffer(new EsProxyClientConsumer());
        requestContext.setProxyConfig(proxyConfig);
        Header[] headerArrays = new Header[1];
        headerArrays[0] = new BasicHeader("Content-Type", "application/json");
        requestContext.setRequestInfo(new RequestContext.RequestInfo(
                new HashMap<>(0),
                headerArrays,
                HttpMethod.GET.name(),
                null,
                ""
        ));
        return requestContext;
    }

    public static class EsHttpConfigCallback implements RestClientBuilder.HttpClientConfigCallback {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
                    .setConnectionRequestTimeout(10 * 1000)
                    .setSocketTimeout(120 * 1000)
                    .build();
            httpClientBuilder.setDefaultRequestConfig(requestConfig);
            try {
                httpClientBuilder.setSSLContext(getSSLContext());
                httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
            } catch (Exception e) {
                log.error("SSL load error. Please check the ca file", e);
            }
            return httpClientBuilder;
        }
    }

    /**
     * 配置 SSL 上下文
     */
    public static SSLContext sslContext = null;

    public static SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
        if (sslContext != null) {
            return sslContext;
        }
        synchronized (RestUtils.class) {
            TrustManager[] trustManagers = new TrustManager[]{new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }};
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagers, null);
            return sslContext;
        }
    }

}
