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

import com.ly.ckibana.constants.Constants;
import com.ly.ckibana.model.exception.UnSupportedDateTypeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import static com.ly.ckibana.constants.Constants.DATETIME_FORMAT_DEFAULT;
import static com.ly.ckibana.constants.Constants.DATETIME_FORMAT_YYYY_MM_DD_HH_MM_SS_SSS;

@Slf4j
public class DateUtils {
    public static final ZoneId ZONE_LOCAL = ZoneId.systemDefault();

    private static final DateTimeFormatter DATETIME_FORMATTER_FOR_GMT = DateTimeFormatter.ofPattern(Constants.DATETIME_FORMAT_GMT).localizedBy(Locale.getDefault());

    private static final DateTimeFormatter DATETIME_FORMATTER_FOR_GMT_WITH_8H = DateTimeFormatter.ofPattern(Constants.DATETIME_FORMAT_GMT_PLUS_EIGHT_HOUR).localizedBy(Locale.getDefault());

    private static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_FORMAT_DEFAULT).localizedBy(Locale.getDefault());

    private static final DateTimeFormatter DEFAULT_DATETIME_WITH_MS_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_FORMAT_YYYY_MM_DD_HH_MM_SS_SSS).localizedBy(Locale.getDefault());

    private static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern(Constants.DATE_FORMAT_DEFAULT).localizedBy(Locale.getDefault());

    private static final List<DateTimeFormatter> DEFAULT_DATE_FORMATTER_LIST = Constants.EXTENDED_DATETIME_FORMAT_LIST
            .stream().map(n -> DateTimeFormatter.ofPattern(n).localizedBy(Locale.getDefault())).toList();

    public static final int ONE_SECOND = 1000;

    /**
     * 将时间戳转换为gmt时间.
     *
     * @param timestamp 时间戳
     * @return gmt时间
     */
    public static String convertTimeStamp2LocalTime(long timestamp) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZONE_LOCAL);
        return DATETIME_FORMATTER_FOR_GMT.format(dateTime);
    }

    /**
     * 时间戳转换为unix time.
     *
     * @param timestamp 时间戳
     * @return unix time
     */
    public static String getGMTOffsetEightHourDateStr(long timestamp) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZONE_LOCAL);
        return DATETIME_FORMATTER_FOR_GMT_WITH_8H.format(dateTime);
    }

    public static String getDefaultDateStr(Date date) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), ZONE_LOCAL);
        return DEFAULT_DATE_FORMATTER.format(dateTime);
    }

    public static String getDefaultDateTimeStr(Date date) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), ZONE_LOCAL);
        return DEFAULT_DATETIME_FORMATTER.format(dateTime);
    }

    /**
     * 时间round 到s.
     * :分割，取最后一位得到ss,如round=10:ss-ss%10  10:10:39 ->10:10:30
     *
     * @param timeMillSeconds timeMillSeconds
     * @param roundSecond     roundSecond
     * @return round后的时间戳
     */
    public static long roundToMSecond(long timeMillSeconds, int roundSecond) {
        try {
            String str = getDefaultDateTimeStr(new Date(timeMillSeconds));
            String[] timeStrArray = str.split(":");
            int second = Integer.parseInt(timeStrArray[timeStrArray.length - 1]);
            int roundNewSecond = second - second % roundSecond;
            String roundNewSecondStr = roundNewSecond < 10 ? "0" + roundNewSecond : String.valueOf(roundNewSecond);
            String newTime = timeStrArray[0] + ":" + timeStrArray[1] + ":" + roundNewSecondStr;
            return toDate(newTime, DEFAULT_DATETIME_FORMATTER).getTime();
        } catch (Exception e) {
            log.error("roundToMSecond失败,使用原始时间, timeMillSeconds:{}, roundSecond:{}", timeMillSeconds, roundSecond, e);
            return timeMillSeconds;
        }
    }

    /**
     * 将时间字符串格式转为时间格式.
     *
     * @param dateStr           时间字符串
     * @param dateTimeFormatter 时间格式
     * @return 时间
     */
    private static Date toDate(String dateStr, DateTimeFormatter dateTimeFormatter) {
        return new Date(toEpochMilli(dateStr, dateTimeFormatter));
    }

    /**
     * 将时间字符串格式转为时间戳.
     *
     * @param dateStr           时间字符串
     * @param dateTimeFormatter 时间格式
     * @return 时间戳
     */
    private static Long toEpochMilli(String dateStr, DateTimeFormatter dateTimeFormatter) {
        return LocalDateTime.parse(dateStr, dateTimeFormatter)
                .atZone(ZONE_LOCAL).toInstant().toEpochMilli();
    }

    private static Long toEpochMilliWithUTC(String dateStr, DateTimeFormatter dateTimeFormatter) {
        return LocalDateTime.parse(dateStr, dateTimeFormatter)
                .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    /**
     * 支持时间转换为时间戳.
     *
     * @param dateTime 时间
     * @return 时间戳
     */
    public static Long toEpochMilli(Object dateTime) {
        if (dateTime instanceof String dateTimeStr) {
            // 非标准化ms时间戳，不走UTC方式，兼容
            if (isNotRegularDateTimeMSFormatValid(dateTimeStr)) {
                return toEpochMilli(dateTimeStr, DEFAULT_DATETIME_WITH_MS_FORMATTER);
            }
            for (DateTimeFormatter formatter : DEFAULT_DATE_FORMATTER_LIST) {
                if (isDateFormatValid(dateTimeStr, formatter)) {
                    return toEpochMilliWithUTC(dateTimeStr, formatter);
                }
            }
        }
        return null;
    }

    /**
     * 日期字符串格式转yyyy-MM-dd格式，支持long,DateTime.
     * 默认返回null
     *
     * @param dateObj 日期对象
     * @return 日期字符串
     */
    public static String getDateStr(Object dateObj) {
        Date date = getDate(dateObj);
        return getDefaultDateStr(date);
    }

    private static Date getDate(Object dateObj) {
        Date date = null;
        if (dateObj instanceof Long dateLongObj) {
            date = new Date(dateLongObj);
        } else if (dateObj instanceof BigInteger dateBigIntegerObj) {
            date = new Date(dateBigIntegerObj.longValue());
        } else if (dateObj instanceof String dateStrObj) {
            for (DateTimeFormatter formatter : DEFAULT_DATE_FORMATTER_LIST) {
                if (isDateFormatValid(dateStrObj, formatter)) {
                    date = toDate((String) dateObj, formatter);
                    break;
                }
            }
        } else if (dateObj instanceof java.sql.Timestamp tsObj) {
            date = new Date(tsObj.getTime());
        }
        if (date == null) {
            throw new UnSupportedDateTypeException("未支持的时间类型" + dateObj.getClass());
        }
        return date;
    }

    /**
     * 时间是否能被当前formatter处理.
     */
    public static boolean isDateFormatValid(String input, DateTimeFormatter formatter) {
        try {
            LocalDateTime.parse(input, formatter);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    public static boolean isNotRegularDateTimeMSFormatValid(String input) {
        try {
            LocalDateTime.parse(input, DEFAULT_DATETIME_WITH_MS_FORMATTER);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    /**
     * 将毫秒格式化为带单位的描述性文本，如d days H hours m minutes s seconds ms millSeconds.
     */
    public static String formatDurationWords(Long milliseconds) {
        if (milliseconds < ONE_SECOND) {
            return String.format("%sms", milliseconds);
        }
        return DurationFormatUtils.formatDurationWords(milliseconds, Boolean.TRUE, Boolean.TRUE);
    }

}
