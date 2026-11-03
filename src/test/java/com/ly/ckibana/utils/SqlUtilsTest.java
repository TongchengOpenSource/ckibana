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

import com.ly.ckibana.util.SqlUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * SQL 工具类测试.
 */
public class SqlUtilsTest {

    @Test
    public void testConvertWildcardToLike() {
        Assert.assertEquals("abc%", SqlUtils.convertWildcardToLike("abc*"));
        Assert.assertEquals("%abc", SqlUtils.convertWildcardToLike("*abc"));
        Assert.assertEquals("a%c", SqlUtils.convertWildcardToLike("a*c"));
        Assert.assertEquals("%a%b%", SqlUtils.convertWildcardToLike("*a*b*"));
        Assert.assertEquals("a_c", SqlUtils.convertWildcardToLike("a?c"));
        Assert.assertEquals("100\\%%", SqlUtils.convertWildcardToLike("100%*"));
        Assert.assertEquals("a*b", SqlUtils.convertWildcardToLike("a\\*b"));
        Assert.assertEquals("a\\\\b", SqlUtils.convertWildcardToLike("a\\\\b"));
        Assert.assertEquals("abc\\\\", SqlUtils.convertWildcardToLike("abc\\"));
    }

    @Test
    public void testWildcardLikeSql() {
        Assert.assertEquals("`f` like 'a\\\\_b%'", SqlUtils.getWildcardLikeSql("`f`", "a_b*"));
        Assert.assertEquals("(lower(`f`) like lower('a\\\\_b%'))", SqlUtils.getCaseIgnoreWildcardSql("`f`", "a_b*"));
    }

    @Test
    public void testContainsWildcard() {
        Assert.assertTrue(SqlUtils.containsWildcard("*"));
        Assert.assertTrue(SqlUtils.containsWildcard("a?c"));
        Assert.assertFalse(SqlUtils.containsWildcard("abc"));
        Assert.assertFalse(SqlUtils.containsWildcard(""));
        Assert.assertFalse(SqlUtils.containsWildcard(null));
    }

    @Test
    public void testNegatedField() {
        Assert.assertTrue(SqlUtils.isNegatedField("-s1"));
        Assert.assertTrue(SqlUtils.isNegatedField("-s1.keyword"));
        Assert.assertFalse(SqlUtils.isNegatedField("-"));
        Assert.assertFalse(SqlUtils.isNegatedField("s1"));
        Assert.assertFalse(SqlUtils.isNegatedField(null));
    }

    @Test
    public void testJoinNegation() {
        Assert.assertEquals(" NOT ", SqlUtils.joinNegation(""));
        Assert.assertEquals(" AND NOT ", SqlUtils.joinNegation(" AND "));
        Assert.assertEquals(" OR NOT ", SqlUtils.joinNegation(" OR "));
        Assert.assertEquals(" AND NOT ", SqlUtils.joinNegation(" AND NOT "));
    }
}
