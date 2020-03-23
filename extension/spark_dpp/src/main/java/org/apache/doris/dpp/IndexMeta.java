// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.dpp;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

enum AggregationType {
    SUM,
    MIN,
    MAX,
    REPLACE,
    REPLACE_IF_NOT_NULL,
    HLL_UNION,
    NONE,
    BITMAP_UNION
}

enum IndexType {
    DUPLICATE,
    AGGREGATE,
    UNIQUE
}

enum ColumnType {
    BOOL,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    LARGEINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATETIME,
    DATE,
    CHAR,
    VARCHAR,
    HLL,
    OBJECT,
    BITMAP
}

class IndexMetaColumnComparator implements Comparator<IndexMeta> {
    @Override
    public int compare(IndexMeta a, IndexMeta b) {
        int diff = a.columns.size() - b.columns.size();
        if (diff == 0) {
            return 0;
        } else if (diff > 0) {
            return 1;
        } else {
            return -1;
        }
    }
}

public class IndexMeta implements Serializable{
    class ColumnDescription implements Serializable{
        @SerializedName("column_name")
        public String columnName;

        @SerializedName("column_type")
        public ColumnType columnType;

        @SerializedName("string_length")
        public int stringLength;

        @SerializedName("is_key")
        public boolean isKey;

        @SerializedName("is_allow_null")
        public boolean isAllowNull;

        @SerializedName("aggregation_type")
        public AggregationType aggregationType;

        @SerializedName("default_value")
        public String defaultValue;
    }

    @SerializedName("columns")
    public List<ColumnDescription> columns;

    @SerializedName("index_id")
    public int indexId;

    @SerializedName("schema_hash")
    public int schemaHash;

    @SerializedName("index_type")
    public IndexType indexType;

    @SerializedName("is_base_index")
    public boolean isBaseIndex;

    public String toString() {
        return "index_id:" + indexId;
    }
}
