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
package com.ly.ckibana.strategy.clause;

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.constants.MsgConstants;
import com.ly.ckibana.model.compute.QueryClause;
import com.ly.ckibana.model.compute.QueryStringField;
import com.ly.ckibana.model.enums.QueryClauseType;
import com.ly.ckibana.model.exception.UiException;
import com.ly.ckibana.model.request.CkRequestContext;
import com.ly.ckibana.model.request.QueryStringClauseParagraphParseContext;
import com.ly.ckibana.model.request.QueryStringClauseParseContext;
import com.ly.ckibana.strategy.clause.converter.QueryStringClauseConverter;
import com.ly.ckibana.util.ParamConvertUtils;
import com.ly.ckibana.util.ProxyUtils;
import com.ly.ckibana.util.SqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

@Component
public class QueryStringClauseStrategy implements ClauseStrategy {

    @Override
    public QueryClauseType getType() {
        return QueryClauseType.QUERY_STRING;
    }

    @Override
    public String toSql(QueryClause queryClause) {
        JSONObject params = queryClause.getParam();
        String queryString = getQueryString(params);
        if (isConditionEmptyOrMatchAll(queryString)) {
            return convertQueryStringToSql(queryClause.getCkRequestContext(), queryString);
        }
        return StringUtils.EMPTY;
    }

    /**
     * 将queryString 按语法分割为段落.
     * 段落值类型：支持字段名，字段值，冒号括号等运算符
     *
     * @param ckRequestContext 请求实体
     * @param queryString        查询sql
     * @return java.lang.String
     * @author quzhihao
     * @date 2023/9/27 15:36
     */
    private String convertQueryStringToSql(CkRequestContext ckRequestContext, String queryString) {
        StringBuilder result = new StringBuilder();
        List<String> queryParagraphs = getSplitParagraphs(queryString);
        List<QueryStringClauseConverter> items = parseToConverters(queryParagraphs, ckRequestContext);
        if (!items.isEmpty()) {
            for (QueryStringClauseConverter each : items) {
                result.append(each.toSql());
            }
        }
        if (StringUtils.isNotBlank(result.toString())) {
            result = new StringBuilder(String.format(" ( %s ) ", result));
        }
        return result.toString();
    }

    /**
     * 基于查询queryString 解析为段落列表返回.
     * 段落值类型：支持字段名，字段值，冒号括号等运算符
     */
    public List<String> getSplitParagraphs(String query) {
        String queryTrimString = StringUtils.trimToEmpty(query);
        // 不支持全字段搜索，因此必须包含:，不包含则提示到ui
        if (!queryTrimString.contains(Constants.Symbol.COLON)) {
            throw new UiException(MsgConstants.QUERY_CONDITION_MISS_MSG);
        }
        QueryStringClauseParagraphParseContext paragraphParseContext = initQueryStringClauseParagraphParseContext(queryTrimString);
        while (paragraphParseContext.getCurrentCharPos() < paragraphParseContext.getQuery().length()) {
            //是否为段落分隔符
            if (isParagraphSeparator(queryTrimString.charAt(paragraphParseContext.getCurrentCharPos()))) {
                //解析段落值信息添加到段落列表，更新最后一位解析字符位置
                collectParagraphsAndUpdateCurrentCharPos(paragraphParseContext);
                //更新下一个段落的起始位置
                paragraphParseContext.setNextParagraphBeginPos(paragraphParseContext.getCurrentCharPos() + 1);
            }
            //更新下一个要解析的字符位置
            paragraphParseContext.setCurrentCharPos(paragraphParseContext.getCurrentCharPos() + 1);
        }
        if (paragraphParseContext.getNextParagraphBeginPos() < queryTrimString.length()) {
            paragraphParseContext.getParagraphs().add(queryTrimString.substring(paragraphParseContext.getNextParagraphBeginPos()));
        }
        return paragraphParseContext.getParagraphs();
    }

    /**
     * 初始化queryString查询上下文.
     */
    private QueryStringClauseParagraphParseContext initQueryStringClauseParagraphParseContext(String queryTrimString) {
        return new QueryStringClauseParagraphParseContext(queryTrimString, new ArrayList<>(), 0, 0);
    }

    /**
     * 基于当前解析字符位置和段落起始位置.
     */
    private void collectParagraphsAndUpdateCurrentCharPos(QueryStringClauseParagraphParseContext queryStringClauseParagraphParseContext) {
        String queryTrimString = queryStringClauseParagraphParseContext.getQuery();
        int paragraphBeginPos = queryStringClauseParagraphParseContext.getNextParagraphBeginPos();
        int currentCharPos = queryStringClauseParagraphParseContext.getCurrentCharPos();
        // 收集段落field
        if (paragraphBeginPos < currentCharPos) {
            queryStringClauseParagraphParseContext.getParagraphs().add(queryTrimString.substring(paragraphBeginPos, currentCharPos));
        }
        //收集有效分隔符(非SINGLE_SPACE_CHAR）
        if (queryTrimString.charAt(currentCharPos) != Constants.Symbol.SINGLE_SPACE_CHAR) {
            queryStringClauseParagraphParseContext.getParagraphs().add(queryTrimString.substring(currentCharPos, currentCharPos + 1));
        }
        //:value格式，且value不为空。range类值:[a TO b]  字符串类值 :c
        if (isColonChar(queryTrimString, currentCharPos, queryTrimString.charAt(currentCharPos))) {
            collectValueParagraphsAndUpdateCharPos(queryStringClauseParagraphParseContext);
        }

    }

    private boolean isColonChar(String queryTrimString, int pos, char c) {
        return c == Constants.Symbol.COLON.charAt(0) && pos + 1 < queryTrimString.length();
    }

    /**
     * 是否为queryString的段落分隔符。空格，左括号，右括号，冒号.
     */
    private boolean isParagraphSeparator(char c) {
        return c == Constants.Symbol.SINGLE_SPACE_CHAR || c == Constants.Symbol.LEFT_PARENTHESIS_CHAR
                || c == Constants.Symbol.RIGHT_PARENTHESIS_CHAR || c == Constants.Symbol.COLON_CHAR;
    }
    
    /**
     * 收集段落分隔符:后面的段落值。并返回最后一个段落的结束位置.
     * isRangeStartChar 是否为range的段落值。如field:[a,b]中的值为[a,b]
     * isInStartChar 是否为in的段落值。如field:(a,b)中的值为(a,b)
     * 空格或最后一位查询字符，均认为当前段落结束 .如field:abc,或AND (field:abc)
     *
     * @param queryStringClauseParagraphParseContext 查询上下文
     */
    private void collectValueParagraphsAndUpdateCharPos(QueryStringClauseParagraphParseContext queryStringClauseParagraphParseContext) {
        String query = queryStringClauseParagraphParseContext.getQuery();
        int colonCharPos = queryStringClauseParagraphParseContext.getCurrentCharPos();
        //:后面空格忽略
        if (isCloneAndSingleSpaceChars(query, colonCharPos)) {
            colonCharPos = colonCharPos + 1;
        }
        int paragraphEndPos = colonCharPos;
        int paragraphBeginIndex = colonCharPos + 1;
        if (isRangeStartChar(query.charAt(colonCharPos + 1))) {
            paragraphEndPos = getParagraphEndIndex(query, colonCharPos, paragraphBeginIndex, Constants.Symbol.RIGHT_BRACKET_CHAR);
            queryStringClauseParagraphParseContext.getParagraphs().add(query.substring(paragraphBeginIndex, Math.min(paragraphEndPos + 1, query.length())));

        } else if (isInStartChar(colonCharPos, query)) {
            paragraphEndPos = getParagraphEndIndex(query, colonCharPos, paragraphBeginIndex, Constants.Symbol.RIGHT_PARENTHESIS_CHAR);
            queryStringClauseParagraphParseContext.getParagraphs().add(query.substring(paragraphBeginIndex, Math.min(paragraphEndPos + 1, query.length())));
        } else {
            for (int pos = paragraphBeginIndex; pos < query.length(); pos++) {
                char currentChar = query.charAt(pos);
                if (currentChar == Constants.Symbol.SINGLE_SPACE_CHAR || isRightParenthesisChar(currentChar)) {
                    queryStringClauseParagraphParseContext.getParagraphs().add(query.substring(colonCharPos + 1, pos));
                    paragraphEndPos = pos - 1;
                    break;
                }
            }
        }

        //更新字符解析位置索引
        queryStringClauseParagraphParseContext.setCurrentCharPos(paragraphEndPos);
    }

    private boolean isRightParenthesisChar(char currentChar) {
        return currentChar == Constants.Symbol.RIGHT_PARENTHESIS_CHAR;
    }

    private boolean isCloneAndSingleSpaceChars(String query, int index) {
        return query.charAt(index) == Constants.Symbol.COLON.charAt(0) && query.charAt(index + 1) == Constants.Symbol.SINGLE_SPACE_CHAR;
    }

    /**
     * 获取一个段落的结束位置.
     */
    private int getParagraphEndIndex(String query, int defaultParagraphEndInex, int paragraphBeginIndex, char paragraphEndChar) {
        int endIndex = defaultParagraphEndInex;
        for (int j = paragraphBeginIndex; j < query.length(); j++) {
            char tempC = query.charAt(j);
            if (tempC == paragraphEndChar) {
                endIndex = j;
                break;
            }
        }
        return endIndex;
    }

    /**
     * queryString收集器.
     * 测试用例：(host:a OR b:1) AND ip:10.100 AND (c:1 OR c:2)
     *
     * @param paragraphs 查询段落
     * @param ckRequestContext 请求上下文
     * @return List
     */
    public List<QueryStringClauseConverter> parseToConverters(List<String> paragraphs, CkRequestContext ckRequestContext) {
        QueryStringClauseParseContext convertContext = initQueryStringClauseParseContext();
        while (convertContext.getCurrentParagraphPos() < paragraphs.size()) {
            convertContext.setPreParagraph(getLastConvertedParagraph(paragraphs, ckRequestContext, convertContext));
            convertContext.setCurrentParagraphPos(convertContext.getCurrentParagraphPos() + 1);
        }
        return convertContext.getConverters();
    }

    /**
     * 获取最后一个被解析完成的段落位置.
     *
     * @param paragraphs        查询段落
     * @param ckRequestContext 请求上下文
     * @param convertContext   解析上下文
     * @return String
     */
    private String getLastConvertedParagraph(List<String> paragraphs, CkRequestContext ckRequestContext,
                                             QueryStringClauseParseContext convertContext) {
        String currentParagraph = paragraphs.get(convertContext.getCurrentParagraphPos());
        String preParagraph = convertContext.getCurrentParagraphPos() > 0 ? paragraphs.get(convertContext.getCurrentParagraphPos() - 1)
                : StringUtils.EMPTY;

        //括号情况下，逻辑运算符和括号都单独作为clause
        if (SqlUtils.isParenthesisSymbol(currentParagraph)) {
            if (StringUtils.isNotBlank(convertContext.getCurrLogicalOp())) {
                convertContext.getConverters().add(new QueryStringClauseConverter(null, null, null, convertContext.getCurrLogicalOp()));
                resetLogicalOp(convertContext);
            }
            convertContext.getConverters().add(new QueryStringClauseConverter(null, null, null, currentParagraph));
        } else if (SqlUtils.isLogicalOperator(currentParagraph)) {
            //逻辑运算符
            convertContext.setCurrLogicalOp(joinLogicalOp(currentParagraph, preParagraph));
        } else if (SqlUtils.isColonSymbol(currentParagraph)) {
            //当前为冒号。截取上一个段落为key
            convertContext.setCurrField(convertContext.getPreParagraph());
        } else if (SqlUtils.isColonSymbol(convertContext.getPreParagraph())) {
            //前一段为冒号，解析value
            currentParagraph = getValueAndUpdateCurrentParagraphPos(convertContext, paragraphs);
            convertContext.getConverters().add(constructConverter(ckRequestContext, convertContext, currentParagraph));
            resetContext(convertContext);
        }
        return currentParagraph;
    }
    
    /**
     * 初始化queryString解析参数.
     *
     * @return QueryStringClauseParseContext
     */
    private QueryStringClauseParseContext initQueryStringClauseParseContext() {
        return new QueryStringClauseParseContext(new ArrayList<>(), StringUtils.EMPTY, StringUtils.EMPTY, StringUtils.EMPTY, 0, 0);
    }
    
    /**
     * 获取逻辑操作符值。支持单或双逻辑操作符.
     * 如AND 或者 AND NOT
     */
    private String joinLogicalOp(String curr, String last) {
        return SqlUtils.wrapFieldSpace(SqlUtils.isLogicalOperator(last) ? String.format("%s %s", last, curr) : curr);
    }

    /**
     * 重置操作符值为空.
     *
     * @param convertContext 解析上下文
     */
    private void resetLogicalOp(QueryStringClauseParseContext convertContext) {
        convertContext.setCurrLogicalOp(StringUtils.EMPTY);
    }

    /**
     * 重置运算符和字段名.
     *
     * @param convertContext 解析上下文
     */
    private void resetContext(QueryStringClauseParseContext convertContext) {
        convertContext.setCurrLogicalOp(StringUtils.EMPTY);
        convertContext.setCurrField(StringUtils.EMPTY);
    }

    private QueryStringClauseConverter constructConverter(CkRequestContext ckRequestContext,
                                                          QueryStringClauseParseContext convertContext, String value) {
        QueryStringField dto = new QueryStringField();
        dto.setName(convertContext.getCurrField());
        dto.setCkName(ParamConvertUtils.convertUiFieldToCkField(ckRequestContext.getColumns(), convertContext.getCurrField()));
        dto.setCkType(ProxyUtils.getCkFieldTypeByName(dto.getCkName(), ckRequestContext.getColumns()));
        return new QueryStringClauseConverter(convertContext.getCurrLogicalOp(), dto, value, null);
    }

    /**
     * 获取queryString的值段落.
     * 比如f:"a,b"中的"a,b"
     */
    private String getValueAndUpdateCurrentParagraphPos(QueryStringClauseParseContext queryStringClauseParseContext,
                                                        List<String> paragraphs) {
        //获取值结束的位置
        updateValueParagraphsEndPos(queryStringClauseParseContext, paragraphs);
        //获取值
        StringJoiner valueJoiner = new StringJoiner(Constants.Symbol.SINGLE_SPACE_STRING);
        for (int j = queryStringClauseParseContext.getCurrentParagraphPos(); j <= queryStringClauseParseContext.getValueParagraphsEndPos(); j++) {
            valueJoiner.add(paragraphs.get(j));
        }
        String value = valueJoiner.toString();
        //更新下一个解析段落位置
        queryStringClauseParseContext.setCurrentParagraphPos(queryStringClauseParseContext.getValueParagraphsEndPos());
        return value;
    }

    /**
     * 获取双引号的值结束段落位置.
     * 如值为"a b"则为关系闭"的位置
     *
     * @param queryStringClauseParseContext  queryString解析上下文
     * @param paragraphs                    查询段落
     */
    private void updateValueParagraphsEndPos(QueryStringClauseParseContext queryStringClauseParseContext,
                                             List<String> paragraphs) {
        int valueParagraphsEndPos = queryStringClauseParseContext.getCurrentParagraphPos();
        String curr = paragraphs.get(queryStringClauseParseContext.getCurrentParagraphPos());
        if (isDoubleQuota(curr)) {
            for (int j = queryStringClauseParseContext.getCurrentParagraphPos() + 1; j < paragraphs.size(); j++) {
                if (paragraphs.get(j).contains(Constants.Symbol.DOUBLE_QUOTA)) {
                    valueParagraphsEndPos = j;
                    break;
                }
            }
        }
        queryStringClauseParseContext.setValueParagraphsEndPos(valueParagraphsEndPos);
    }

    /**
     * 是否为双引号开始的有效值.
     *
     * @param curr curr
     * @return boolean
     */
    private boolean isDoubleQuota(String curr) {
        return curr.startsWith(Constants.Symbol.DOUBLE_QUOTA) && !curr.endsWith(Constants.Symbol.DOUBLE_QUOTA);
    }

    /**
     * range类值[a TO b].
     * 字符串类值 c
     */
    private boolean isRangeStartChar(char c) {
        return c == Constants.Symbol.LEFT_BRACKET_CHAR;
    }

    /**
     * 判断是否为满足in查询条件.
     * (request_uri:空格("a","b")) 或无空格(request_uri:("a","b"))
     *
     * @param index 当前位置
     * @param query 查询语句
     * @return 是否在query中
     */
    private boolean isInStartChar(int index, String query) {
        if (index < query.length()
                && index + 1 < query.length()
                && index + 2 < query.length()) {
            return query.charAt(index + 1) == Constants.Symbol.SINGLE_SPACE_CHAR
                    && query.charAt(index + 2) == Constants.Symbol.LEFT_PARENTHESIS_CHAR
                    || query.charAt(index + 1) == Constants.Symbol.LEFT_PARENTHESIS_CHAR;
        }
        return false;
    }

    private boolean isConditionEmptyOrMatchAll(String queryString) {
        return !Constants.MATCH_ALL.equals(queryString) && StringUtils.isNotBlank(queryString);
    }

    private String getQueryString(JSONObject obj) {
        if (obj.containsKey(Constants.QUERY)) {
            return obj.getString(Constants.QUERY);
        }
        return "";
    }
}
