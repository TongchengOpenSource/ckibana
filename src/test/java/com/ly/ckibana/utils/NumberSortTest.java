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

import com.alibaba.fastjson2.JSONObject;
import com.ly.ckibana.model.enums.SortType;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * mathBucket值排序转换
 *
 * @author zl11357
 * @since 2023/10/19 22:17
 */
public class NumberSortTest {
    /**
     * 大值排序测试
     */
    @Test
    public void superLongNumberTest() {
        Long bigValue = 523412341234L;
        Long littleValue = 423412341234L;
        JSONObject row = new JSONObject();
        row.put("v1", bigValue.toString());
        row.put("v2", littleValue.toString());
        Assert.assertTrue("升序测试",sort(row, SortType.ASC).equals(Arrays.asList(littleValue, bigValue).toString()));
        Assert.assertTrue("降序测试", sort(row, SortType.DESC).equals(Arrays.asList(bigValue, littleValue).toString()));
    }

    /**
     * 小值排序测试
     */
    @Test
    public void superIntNumberTest() {
        Integer bigValue = 20;
        Integer littleValue = 10;
        JSONObject row = new JSONObject();
        row.put("v1", bigValue.toString());
        row.put("v2", littleValue.toString());
        Assert.assertTrue("升序测试",sort(row, SortType.ASC).equals(Arrays.asList(littleValue, bigValue).toString()));
        Assert.assertTrue("降序测试", sort(row, SortType.DESC).equals(Arrays.asList(bigValue, littleValue).toString()));
    }

    private String sort(JSONObject row, SortType sortType) {
        return Arrays.asList(new BigDecimal(row.getString("v1")), new BigDecimal(row.getString("v2"))).stream()
                .sorted((o1, o2) -> {
                    int compareValue = (o1.compareTo(o2));
                    return sortType.equals(SortType.ASC) ? compareValue : -compareValue;
                }).collect(Collectors.toList()).toString();
    }
}
