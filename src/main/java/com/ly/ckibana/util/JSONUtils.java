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

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapperImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public final class JSONUtils {

    /**
     * 序列化.
     */
    public static String serialize(Object o) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(o);
        } catch (Exception ex) {
            log.error("serialize error", ex);
            return "{}";
        }
    }

    /**
     * 反序列化.
     */
    public static <T> T deserialize(String json, Class<T> type) {
        try {
            return JSON.parseObject(json, type);
        } catch (Exception ex) {
            log.error("deserialize error, json:{}", json, ex);
            throw ex;
        }
    }

    /**
     * 反序列化为list.
     */
    public static <T> List<T> deserializeToList(String json, Class<T> tClass) {
        List<T> result = new ArrayList<>();
        deserialize(json, List.class).stream().forEach(each -> result.add(convert(each, tClass)));
        return result;
    }

    /**
     * list转换.
     */
    public static <T> List<T> deserializeToList(List<?> list, Class<T> tClass) {
        if (list == null) {
            return new ArrayList<>();
        }
        return list.stream().map(o -> convert(o, tClass)).collect(Collectors.toList());
    }

    /**
     * object转换为map.
     */
    public static Map convertToMap(Object o) {
        return convert(o, Map.class);
    }

    /**
     * object转换为其他类型.
     */
    public static <T> T convert(Object o, Class<T> type) {
        try {
            if (o == null) {
                return null;
            }
            String json = JSON.toJSONString(o);
            return JSON.parseObject(json, type);
        } catch (Exception ex) {
            log.error("convert error", ex);
            throw ex;
        }
    }

    /**
     * 拷贝生成同类新对象.
     */
    public static <T> T copy(Object source) {
        Object result = new BeanWrapperImpl(source.getClass()).getWrappedInstance();
        BeanUtils.copyProperties(source, result);
        return (T) result;
    }
}
