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
package com.ly.ckibana.utils;

import com.ly.ckibana.util.DateUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * 时间round测试
 *
 * @author zl11357
 * @since 2023/10/19 22:17
 */
public class DateUtilsTest {
    //2023-10-26 08:59:10 1698281950330L
    long t1Origin = 1698281950330L;
    //2023-10-26 08:59:00 1698281940000L
    long t2Origin = 1698281940000L;
    //2023-10-26 08:59:11 1698281951000L
    long t3Origin = 1698281951000L;

    /**
     * 时间戳转gmt时间测试
     */
    @Test
    public void convertTimeStamp2GMTTimeTest() {
        Assert.assertTrue("convertTimeStamp2GMTTimeTest" + t1Origin, "2023-10-26T08:59:10.330Z".equals(DateUtils.convertTimeStamp2LocalTime(t1Origin)));
        Assert.assertTrue("convertTimeStamp2GMTTimeTest" + t2Origin, "2023-10-26T08:59:00.000Z".equals(DateUtils.convertTimeStamp2LocalTime(t2Origin)));
        Assert.assertTrue("convertTimeStamp2GMTTimeTest", "2023-10-26T08:59:11.000Z".equals(DateUtils.convertTimeStamp2LocalTime(t3Origin)));

    }

    /**
     * 时间round 10s测试
     */
    @Test
    public void roundTest() {
        int roundSecond = 10;
        Assert.assertTrue("roundTest" + t1Origin, DateUtils.roundToMSecond(t1Origin, roundSecond) == 1698281950000L);
        Assert.assertTrue("roundTest" + t2Origin, DateUtils.roundToMSecond(t2Origin, roundSecond) == 1698281940000L);
        Assert.assertTrue("roundTest" + t3Origin, DateUtils.roundToMSecond(t3Origin, roundSecond) == 1698281950000L);
    }

    /**
     * 测试时间转为描述性提示。几天几小时格式
     */
    @Test
    public void formatDurationWordsTest() {
        long millSeconds = 100;
        long seconds = 2 * 1000 + millSeconds;
        long min = 3 * 60 * 1000 + seconds;
        long hour = 4 * 60 * 60 * 1000 + min;
        long day = 5 * 24 * 60 * 60 * 1000 + hour;

        Assert.assertTrue("formatDurationWordsTest ms正常", "100ms".equals(DateUtils.formatDurationWords(millSeconds)));
        Assert.assertTrue("formatDurationWordsTest s正常", "2 seconds".equals(DateUtils.formatDurationWords(seconds)));
        Assert.assertTrue("formatDurationWordsTest min正常", "3 minutes 2 seconds".equals(DateUtils.formatDurationWords(min)));
        Assert.assertTrue("formatDurationWordsTest hour正常", "4 hours 3 minutes 2 seconds".equals(DateUtils.formatDurationWords(hour)));
        Assert.assertTrue("formatDurationWordsTest day正常", "5 days 4 hours 3 minutes 2 seconds".equals(DateUtils.formatDurationWords(day)));
    }

    @Test
    public void toEpochMilliTest() {
        String utc = "2024-07-12T08:46:21.659Z";
        Locale locale = Locale.getDefault();
        long ts1 = LocalDateTime.parse(utc, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").localizedBy(locale)).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        Assert.assertEquals(ts1, 1720773981659L);

        Instant instant = Instant.parse(utc);
        ZoneId shanghaiZoneId = ZoneId.of("Asia/Shanghai");
        ZonedDateTime shanghaiTime = instant.atZone(shanghaiZoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        String formattedShanghaiTime = shanghaiTime.format(formatter);
        Assert.assertEquals(formattedShanghaiTime, "2024-07-12T16:46:21.659Z");
    }

    @Test
    public void notRegularDateTimeMSFormatValidTest() {
        String dataTime1 = "2024-07-12'T'08:46:21.659";
        String dataTime2 = "2024-07-12T08:46:21.659";
        String dataTime3 = "2024-07-12 08:46:21";
        String dataTime4 = "2024-07-12";
        String dataTime5 = "2024-07-12 08:46:21.000";
        Assert.assertFalse(DateUtils.isNotRegularDateTimeMSFormatValid(dataTime1));
        Assert.assertFalse(DateUtils.isNotRegularDateTimeMSFormatValid(dataTime2));
        Assert.assertFalse(DateUtils.isNotRegularDateTimeMSFormatValid(dataTime3));
        Assert.assertFalse(DateUtils.isNotRegularDateTimeMSFormatValid(dataTime4));
        Assert.assertTrue(DateUtils.isNotRegularDateTimeMSFormatValid(dataTime5));
    }

}
