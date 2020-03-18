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

import org.apache.spark.Partitioner;

import java.util.List;

public class DorisRangePartitioner extends Partitioner {
    private int partitionNum;
    private List<DppColumns> partitionRangeKeys;
    private List<String> partitionColums;
    List<Integer> partitionKeyIndexes;
    public DorisRangePartitioner(PartitionInfo partitionInfo, List<Integer> partitionKeyIndexes, List<DppColumns> partitionRangeKeys) {
        partitionNum = partitionInfo.partitions.size();
        partitionColums = partitionInfo.partitionColumnNames;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.partitionRangeKeys = partitionRangeKeys;
    }

    public int numPartitions() {
        return partitionNum;
    }

    public int getPartition(Object var1) {
        DppColumns key = (DppColumns)var1;
        // get the partition columns from key as partition key
        DppColumns partitionKey = new DppColumns(key, partitionKeyIndexes);
        for (int i = partitionRangeKeys.size() - 1; i >= 0; --i) {
            if (partitionKey.compareTo(partitionRangeKeys.get(i)) > 0) {
                return i;
            }
        }
        return -1;
    }
}
