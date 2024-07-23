package com.ly.ckibana.constants;

import java.util.Arrays;
import java.util.List;

/**
 * es常量
 *
 * @Author: zl11357
 * @Email: zl11357@ly.com
 * @Date: 2024/7/19
 */
public class EsConstants {
    /**
     * es 7+ 默认排序字段。
     */
    public static final String META_FIELD_DOC = "_doc";
    /**
     * es 6 默认排序字段。
     */
    public static final String META_FIELD_SCORE = "_score";
    /**
     * es所有版本默认排序参数。实际对ck无用
     */
    public static final List<String> KIBANA_DEFAULT_SORT_FILEDS = Arrays.asList(META_FIELD_DOC, META_FIELD_SCORE);


}
