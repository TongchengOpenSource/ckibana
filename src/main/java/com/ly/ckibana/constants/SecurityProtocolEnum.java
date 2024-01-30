package com.ly.ckibana.constants;

import java.util.Arrays;

/**
 * 协议枚举类
 */
public enum SecurityProtocolEnum {
    /**
     * HTTP
     */
    HTTP("http", 80),
    /**
     * HTTPS
     */
    HTTPS("https", 443);

    /**
     * 协议名
     */
    private final String scheme;
    /**
     * 默认端口号
     */
    private final int port;

    SecurityProtocolEnum(String scheme, int port) {
        this.scheme = scheme;
        this.port = port;
    }

    public String getScheme() {
        return scheme;
    }

    public int getPort() {
        return port;
    }

    public SecurityProtocolEnum get(String scheme) {
        return Arrays.stream(values()).filter(n -> n.getScheme().equals(scheme)).findFirst().orElse(null);
    }
}
