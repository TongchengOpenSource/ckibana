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
package com.ly.ckibana.model.request;

import com.ly.ckibana.constants.SqlConstants;
import com.ly.ckibana.util.SqlUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class CkRequest {

    private String table;

    private String select;

    private String where;

    private String sample;

    private String sorting;

    private String group;

    private String having;

    private String limit;

    public CkRequest() {
    }

    public void initSelect(String requiredData) {
        if (StringUtils.isNotBlank(requiredData)) {
            setSelect(requiredData);
        }
    }

    /**
     * 追加select字段.
     *
     * @param requiredData requiredData
     */
    public void appendSelect(String requiredData) {
        if (StringUtils.isNotBlank(getSelect()) && StringUtils.isNotBlank(requiredData)) {
            setSelect(getSelect() + ", " + requiredData);
        } else {
            initSelect(requiredData);
        }
    }

    public void initGroupBy(String requiredData) {
        if (StringUtils.isNotBlank(requiredData)) {
            setGroup(String.format(SqlConstants.GROUP_BY_TEMPLATE, requiredData));
        }
    }

    public void appendGroupBy(String requiredData) {
        if (StringUtils.isNotBlank(requiredData)) {
            if (StringUtils.isBlank(getGroup())) {
                initGroupBy(requiredData);
            } else {
                setGroup(String.format("%s,%s", getGroup(), requiredData));
            }
        }
    }

    public void limit(int limit) {
        setLimit(String.format(SqlConstants.LIMIT_TEMPLATE, limit));
    }

    public void resetWhere() {
        setWhere(StringUtils.EMPTY);
    }

    public void appendWhere(String where) {
        if (StringUtils.isNotBlank(where) && StringUtils.isNotBlank(getWhere())) {
            setWhere(getWhere() + String.format(" AND (%s)", where));
            return;
        }
        if (StringUtils.isNotBlank(where)) {
            setWhere(String.format(SqlConstants.PREWHERE_TEMPLATE, where));
        }
    }

    public void orderBy(String sort) {
        if (StringUtils.isNotBlank(sort)) {
            if (sort.startsWith("ORDER BY")) {
                setSorting(sort);
            } else {
                setSorting(String.format(SqlConstants.ORDER_BY_TEMPLATE, sort));
            }
        }
    }

    public String buildToStr() {
        StringBuilder builder = new StringBuilder();
        builder.append(SqlUtils.getSelectTemplate(getSelect(), getTable()));
        if (StringUtils.isNotBlank(getSample())) {
            builder.append(" ").append(getSample());
        }
        if (StringUtils.isNotBlank(getWhere())) {
            builder.append(" ").append(getWhere());
        }
        if (StringUtils.isNotBlank(getGroup())) {
            builder.append(" ").append(getGroup());
        }
        if (StringUtils.isNotBlank(getHaving())) {
            builder.append(" ").append(getHaving());
        }
        if (StringUtils.isNotBlank(getSorting())) {
            builder.append(" ").append(getSorting());
        }
        if (StringUtils.isNotBlank(getLimit())) {
            builder.append(" ").append(getLimit());
        }
        return builder.toString();
    }
}
