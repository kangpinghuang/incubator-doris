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

enum PartitionType {
    RANGE,
    HASH
}

public class PartitionInfo implements Serializable {
    @SerializedName("partition_type")
    public PartitionType partitionType;

    @SerializedName("partition_column_refs")
    public List<String> partitionColumnNames;

    class PartitionDescription implements Serializable {
        @SerializedName("partition_id")
        int partitionId;

        @SerializedName("start_keys")
        public List<Long> startKeys;

        @SerializedName("end_keys")
        public List<Long> endKeys;

        @SerializedName("is_max_partition")
        public boolean isMaxPartition;

        @SerializedName("bucket_num")
        public int bucketNum;
    }

    @SerializedName("partitions")
    public List<PartitionDescription> partitions;

    @SerializedName("distribution_column_refs")
    public List<String> distributionColumnRefs;
}
