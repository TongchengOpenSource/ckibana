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

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 工具类.
 *
 * @author caojiaqiang
 */
@Service
public class Utils {
    
    public static final int DEFAULT_DOUBLE_SCALE = 6;

    private static final String IPV4_PATTERN = "(((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))))";

    private static final String IPV6_PATTERN = "([\\da-fA-F]{1,4}:){7}[\\da-fA-F]{1,4}|:((:[\\da−fA−F]1,4)1,6|:)";
    
    /**
     * 修剪字符串前缀、后缀.
     * @param value value
     * @param beginChar beginChar
     * @param endChar endChar
     * @return 修剪后的字符串
     */
    public static String trimByPrefixAndSuffixChars(String value, String beginChar, String endChar) {
        String result;
        result = trimPrefix(value, beginChar);
        result = trimSuffix(result, endChar);
        return result;
    }

    /**
     * 修剪字符串前缀.
     * @param value value
     * @param prefix prefix
     * @return 修剪后的字符串
     */
    public static String trimPrefix(String value, String prefix) {
        String result = value;
        if (value.startsWith(prefix)) {
            result = StringUtils.substring(value, prefix.length());
        }
        return result;
    }

    /**
     * 修剪字符串后缀.
     * @param value value
     *  @param suffix suffix
     *  @return 修剪后的字符串
     */
    public static String trimSuffix(String value, String suffix) {
        String result = value;
        if (value.endsWith(suffix)) {
            result = StringUtils.substring(value, 0, value.length() - suffix.length());
        }
        return result;
    }
    
    /**
     * 数组转为列表.
     * @param items items
     * @return List
     */
    public static List<String> addNotBlankItemsToList(String... items) {
        List<String> result = new ArrayList<>();
        for (String each : items) {
            if (StringUtils.isNotBlank(each)) {
                result.add(each);
            }
        }
        return result;
    }
    
    /**
     * double保留n位小数，可防止出现科学计数法E.
     */
    public static BigDecimal double2Scale(double value) {
        BigDecimal bigDecimal = new BigDecimal(value);
        return bigDecimal.setScale(DEFAULT_DOUBLE_SCALE, RoundingMode.HALF_UP);
    }
    
    /**
     * 将字符串转换为UUID.
     * @param str str
     * @return String
     */
    public static String toUuid(String str) {
        return UUID.nameUUIDFromBytes(str.getBytes(StandardCharsets.UTF_8)).toString().replace("-", "");
    }

    public static String getRandomUuid() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    
    /**
     * 获取索引名称.
     * @param indexName indexName
     * @param datePattern datePattern
     * @return String
     */
    public static String getIndexName(String indexName, String datePattern) {
        return String.format("%s-%s", indexName, LocalDate.now().format(DateTimeFormatter.ofPattern(datePattern)));
    }
    
    /**
     * 是否是IPV4地址.
     * @param value value
     * @return boolean
     */
    public static boolean isIPv4Value(String value) {
        return Pattern.matches(IPV4_PATTERN, value);
    }

    /**
     * 是否是IPV6地址.
     *
     * @param value value
     * @return boolean
     */
    public static boolean isIPv6Value(String value) {
        return Pattern.matches(IPV6_PATTERN, value);
    }

}
