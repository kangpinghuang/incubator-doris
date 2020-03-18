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
import java.util.List;
import java.util.Map;

public class SourceDescription implements Serializable {
    public List<Integer> partitions;

    @SerializedName("file_urls")
    public List<String> fileUrls;

    public List<String> columns;

    @SerializedName("column_seperator")
    public String columnSeperator;

    @SerializedName("columns_from_path")
    public List<String> columnsFromPath;

    class ColumnMapping implements Serializable {
        @SerializedName("function_name")
        public String functionName;

        public List<String> arguments;
    }

    @SerializedName("column_mappings")
    public Map<String, ColumnMapping> columnMappings;

    @SerializedName("where")
    public String where;

    @SerializedName("is_negative")
    public boolean isNegative;

    // used for global dict
    @SerializedName("hive_table_name")
    public String hiveTableName;
}
