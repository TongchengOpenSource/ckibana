package com.ly.ckibana.model.response;

import lombok.Data;

import java.util.List;

/**
 * 索引检查响应体
 *
 * @author kizuhiko
 */
@Data
public class IndexCheckResponse {
    private String index;
    private List<String> databaseUrls;
    private boolean directToEs;
    private List<String> hitTables;
    private boolean inWhiteList;
    private List<String> whiteList;
    private String sql;
}
