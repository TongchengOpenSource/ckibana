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

import com.ly.ckibana.util.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IpPatternTest {

    @Test
    public void testIpv4() {
        List<String> ipv4 = Arrays.asList("1.1.1.1", "2.2.2.2");
        Assert.assertTrue(ipv4.stream().filter(v -> !Utils.isIPv4Value(v)).count() == 0);
    }

    @Test
    public void testIpv6() {
        List<String> ipv6 = Arrays.asList("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        Assert.assertTrue(ipv6.stream().filter(v -> !Utils.isIPv6Value(v)).count() == 0);
    }
}
