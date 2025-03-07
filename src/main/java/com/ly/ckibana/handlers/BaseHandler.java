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
import com.ly.ckibana.configure.web.mapping.PathTrieHandlerMapping;
import com.ly.ckibana.configure.web.route.HttpRoute;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.exception.FallbackToEsException;
import com.ly.ckibana.model.exception.InitializationException;
import com.ly.ckibana.model.exception.UiException;
import com.ly.ckibana.model.request.ProxyConfig;
import com.ly.ckibana.model.request.RequestContext;
import com.ly.ckibana.model.request.RequestContext.RequestInfo;
import com.ly.ckibana.service.EsClientUtil;
import com.ly.ckibana.util.HttpServletUtils;
import com.ly.ckibana.util.JSONUtils;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.RestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.util.UriUtils;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public abstract class BaseHandler implements CorsConfigurationSource {

    public static final Logger log = LoggerFactory.getLogger(BaseHandler.class);

    public static final CorsConfiguration CORS_CONFIGURATION = new CorsConfiguration().applyPermitDefaultValues();

    @Resource
    private ProxyConfigLoader proxyConfigLoader;

    public List<HttpRoute> routes() {
        return Collections.emptyList();
    }

    public void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, String> urlParams =
                (Map<String, String>) Optional.ofNullable(request.getAttribute(PathTrieHandlerMapping.URL_PARAMS)).orElse(Collections.emptyMap());
        String responseContent;
        RequestContext context = createContext(request, response, urlParams);
        try {
            responseContent = doHandle(context);
        } catch (FallbackToEsException e) {
            // fallback to es
            responseContent = EsClientUtil.doRequest(context);
        } catch (UiException uiException) {
            // display ex msg in a friendly way on kibana.
            responseContent = JSONUtils.serialize(ProxyUtils.newKibanaException(uiException.getUiShow()));
        } catch (Exception e) {
            responseContent = ProxyUtils.getErrorResponse(e);
            log.error(responseContent, e);
            logRequest(context);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        setResponseHeaders(response);
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] {}", response.getStatus(), request.getMethod(), request.getRequestURI(), responseContent);
        }
        IOUtils.write(responseContent, response.getOutputStream(), StandardCharsets.UTF_8);
    }

    private void setResponseHeaders(HttpServletResponse response) {
        if (response.getContentType() == null) {
            response.setContentType(ContentType.APPLICATION_JSON.toString());
        }
        if (response.getHeader(Constants.HEADER_X_ELASTIC_PRODUCT) == null) {
            response.setHeader(Constants.HEADER_X_ELASTIC_PRODUCT, Constants.HEADER_ELASTICSEARCH);
        }
    }

    protected String doHandle(RequestContext context) throws Exception {
        throw new FallbackToEsException();
    }

    @Override
    public CorsConfiguration getCorsConfiguration(HttpServletRequest request) {
        List<String> httpMethodList = routes().stream().map(HttpRoute::getMethods).flatMap(Collection::stream).distinct().map(HttpMethod::name).toList();
        CorsConfiguration corsConfig = new CorsConfiguration(CORS_CONFIGURATION);
        corsConfig.setAllowedMethods(httpMethodList);
        return corsConfig;
    }

    protected RequestContext createContext(HttpServletRequest request, HttpServletResponse response, Map<String, String> urlParams) {
        ProxyConfig proxyConfig = proxyConfigLoader.getConfig();
        if (proxyConfig == null) {
            throw new InitializationException("proxy config is null, please check the index [proxy-settings]");
        }
        return build(request, response, proxyConfig, urlParams);
    }

    private RequestContext build(HttpServletRequest request, HttpServletResponse response, ProxyConfig proxyConfig, Map<String, String> urlParams) {
        String requestBody;
        try {
            requestBody = IOUtils.toString(request.getReader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // request header, default header
        Map<String, Header> reqHeaders = HttpServletUtils.parseHttpRequestHeaders(request);
        Map<String, String> defaultHeaders = proxyConfig.getKibanaItemProperty().getEs().getHeaders();
        defaultHeaders.forEach((dhn, dhv) -> {
            if (dhn.equalsIgnoreCase(Constants.Headers.AUTHORIZATION)) {
                return;
            }

            for (String headerName : reqHeaders.keySet()) {
                if (headerName.equalsIgnoreCase(dhn)) {
                    reqHeaders.put(headerName, new BasicHeader(headerName, dhv));
                }
            }
        });
        Map<String, String> requestParams = HttpServletUtils.parseHttpRequestParams(request);
        RequestInfo requestInfo = new RequestInfo(requestParams, reqHeaders.values().toArray(new Header[0]), request.getMethod(), request.getRequestURI(), requestBody);
        String originalIndex = StringUtils.defaultIfBlank(urlParams.get(Constants.INDEX_NAME_KEY), requestParams.get(Constants.INDEX_NAME_KEY));
        RequestContext context = new RequestContext()
                .setHttpRequest(request)
                .setHttpResponse(response)
                .setUrlParams(urlParams)
                .setProxyConfig(proxyConfig)
                .setRequestUrl(request.getRequestURI())
                .setTargetUrl(request.getRequestURI())
                .setClientIp(RestUtils.getClientIpAddr(request))
                .setRequestInfo(requestInfo);
        return initIndexInfo(context, originalIndex);
    }

    /**
     * init index info for request context.
     *
     * @param requestContext request context
     * @param originalIndex  original index
     * @return request context
     */
    public RequestContext initIndexInfo(RequestContext requestContext, String originalIndex) {
        String index = ProxyUtils.trimRemoteCluster(originalIndex);
        boolean mayBeCkIndex = !requestContext.getProxyConfig().isDirectToEs(index);
        requestContext.setOriginalIndex(originalIndex)
                .setIndex(index)
                .setCkIndex(mayBeCkIndex);
        return requestContext;
    }

    private void logRequest(RequestContext context) {
        if (log.isDebugEnabled()) {
            RequestContext.RequestInfo contextRequestInfo = context.getRequestInfo();
            StringBuilder requestInfoLog = new StringBuilder("Request Info => ")
                    .append(contextRequestInfo.getMethod()).append(" ")
                    .append(UriUtils.decode(context.getHttpRequest().getRequestURI(), StandardCharsets.UTF_8));
            if (contextRequestInfo.getHeaders().length > 0) {
                requestInfoLog.append("\n").append("request headers => ").append(Arrays.toString(contextRequestInfo.getHeaders()));
            }
            if (!contextRequestInfo.getParams().isEmpty()) {
                requestInfoLog.append("\n").append("request params => ").append(contextRequestInfo.getParams());
            }
            if (!StringUtils.isEmpty(contextRequestInfo.getRequestBody())) {
                requestInfoLog.append("\n").append("request body => ").append(contextRequestInfo.getRequestBody());
            }
            log.info("requestInfoLog:{}", requestInfoLog);
        }
    }
}
