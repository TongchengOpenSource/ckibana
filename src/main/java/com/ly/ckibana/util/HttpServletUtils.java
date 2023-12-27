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

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;

import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HttpServlet工具类.
 * @Author: caojiaqiang
 * @createTime: 2023/9/25 11:21 AM
 * @version: 1.0
 * @Description:
 */
public class HttpServletUtils {
    
    public static final List<String> DISCARD_HEADER_NAMES = List.of(
            HttpHeaders.CONTENT_LENGTH,
            HttpHeaders.HOST,
            HttpHeaders.CONNECTION,
            HttpHeaders.USER_AGENT,
            HttpHeaders.ACCEPT_ENCODING,
            HttpHeaders.TRANSFER_ENCODING
    );

    /**
     * 透传http请求参数.
     */
    public static Map<String, String> parseHttpRequestParams(HttpServletRequest httpServletRequest) {
        Map<String, String> result = new HashMap<>();
        if (httpServletRequest != null) {
            Enumeration<String> paramNames = httpServletRequest.getParameterNames();
            while (paramNames.hasMoreElements()) {
                String paramName = paramNames.nextElement();
                result.put(paramName, httpServletRequest.getParameter(paramName));
            }
        }
        return result;
    }

    /**
     * 透传原请求headers.
     * 1.透传原请求headers
     * 2.或非http请求（proxy内部主动发起请求）
     */
    public static Map<String, Header> parseHttpRequestHeaders(HttpServletRequest httpServletRequest) {
        Map<String, Header> headerMap = new HashMap<>();
        if (httpServletRequest != null) {
            Enumeration<String> headerNames = httpServletRequest.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                if (!isDiscardHeader(headerName)) {
                    headerMap.put(headerName, new BasicHeader(headerName, httpServletRequest.getHeader(headerName)));
                }
            }
        } else {
            //非http请求（proxy内部主动发起请求），添加默认header
            headerMap.put(HttpHeaders.CONTENT_TYPE, new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType()));
        }
        return headerMap;
    }

    public static boolean isDiscardHeader(String headerName) {
        return DISCARD_HEADER_NAMES.stream().anyMatch(discardHeaderName -> discardHeaderName.equalsIgnoreCase(headerName));
    }
}
