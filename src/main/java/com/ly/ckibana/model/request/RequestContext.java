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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.apache.http.Header;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * 请求上下文.
 * @author caojiaqiang
 */
@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class RequestContext {

    private HttpServletRequest httpRequest;

    private HttpServletResponse httpResponse;

    private Map<String, String> urlParams;

    private ProxyConfig proxyConfig;

    private RequestInfo requestInfo;

    private String clientIp;

    private String targetUrl;

    private String requestUrl;

    /**
     * 去掉 originalIndex 中的 remote cluster，比如 remote_cluster_name:index_name -> index_name.
     */
    private String index;

    /**
     * url路径中的原始index，可能包含 remote cluster，比如 remote_cluster_name:index_name.
     */
    private String originalIndex;

    /**
     * 是否匹配 proxy config 的黑白名单，属于 ck 索引.
     */
    private boolean ckIndex;

    @Data
    @AllArgsConstructor
    public static class RequestInfo {
        private Map<String, String> params;

        private Header[] headers;

        private String method;

        private String url;

        private String requestBody;
    }
}
