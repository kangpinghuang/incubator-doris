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

package org.apache.doris.load.loadv2.etl;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** jobconfig.json file format
 * {
 * 	"tables": {
 * 		10014: {
 * 			"indexes": [{
 * 			    "index_id": 10014,
 * 				"columns": [{
 * 				    "column_name": "k1",
 * 				    "column_type": "SMALLINT",
 * 				    "is_key": true,
 * 				    "is_allow_null": true,
 * 				    "aggregation_type": "NONE"
 *                }, {
 * 					"column_name": "k2",
 * 				    "column_type": "VARCHAR",
 * 				    "string_length": 20,
 * 					"is_key": true,
 * 				    "is_allow_null": true,
 * 					"aggregation_type": "NONE"
 *                }, {
 * 					"column_name": "v",
 * 				    "column_type": "BIGINT",
 * 					"is_key": false,
 * 				    "is_allow_null": false,
 * 					"aggregation_type": "NONE"
 *              }],
 * 				"schema_hash": 1294206574,
 * 			    "index_type": "DUPLICATE",
 * 			    "is_base_index": true
 *            }, {
 * 			    "index_id": 10017,
 * 				"columns": [{
 * 				    "column_name": "k1",
 * 				    "column_type": "SMALLINT",
 * 					"is_key": true,
 * 				    "is_allow_null": true,
 * 					"aggregation_type": "NONE"
 *                }, {
 * 					"column_name": "v",
 * 				    "column_type": "BIGINT",
 * 					"is_key": false,
 * 				    "is_allow_null": false,
 * 					"aggregation_type": "SUM"
 *              }],
 * 				"schema_hash": 1294206575,
 * 			    "index_type": "AGGREGATE",
 * 			    "is_base_index": false
 *          }],
 * 			"partition_info": {
 * 				"partition_type": "RANGE",
 * 				"partition_column_refs": ["k1"],
 *              "distribution_column_refs": ["k2"],
 * 				"partitions": [{
 * 				    "partition_id": 10020,
 * 					"start_keys": [-100],
 * 					"end_keys": [10],
 * 					"is_max_partition": false,
 * 					"bucket_num": 3
 *                }, {
 *                  "partition_id": 10021,
 *                  "start_keys": [10],
 *                  "end_keys": [100],
 *                  "is_max_partition": false,
 *  				"bucket_num": 3
 *              }]
 *          },
 * 			"file_groups": [{
 * 		        "partitions": [10020],
 * 				"file_paths": ["hdfs://hdfs_host:port/user/palo/test/file"],
 * 				"file_field_names": ["tmp_k1", "k2"],
 * 				"value_separator": ",",
 * 			    "line_delimiter": "\n",
 * 				"column_mappings": {
 * 					"k1": {
 * 						"function_name": "strftime",
 * 						"args": ["%Y-%m-%d %H:%M:%S", "tmp_k1"]
 *                   }
 *              },
 * 				"where": "k2 > 10",
 * 				"is_negative": false,
 * 				"hive_table_name": "hive_db.table"
 *          }]
 *      }
 *  },
 * 	"output_path": "hdfs://hdfs_host:port/user/output/10003/label1/1582599203397",
 * 	"output_file_pattern": "label1.%d.%d.%d.%d.%d.parquet",
 * 	"label": "label0",
 * 	"properties": {
 * 	    "strict_mode": false,
 * 	    "timezone": "Asia/Shanghai"
 * 	}
 * }
 */
public class EtlJobConfig implements Serializable {
    // global dict
    public static final String GLOBAL_DICT_TABLE_NAME = "doris_global_dict_table_%d";
    public static final String DISTINCT_KEY_TABLE_NAME = "doris_distinct_key_table_%d_%s";
    public static final String DORIS_INTERMEDIATE_HIVE_TABLE_NAME = "doris_intermediate_hive_table_%d_%s";

    // hdfsEtlPath/jobs/dbId/loadLabel/PendingTaskSignature
    private static final String ETL_OUTPUT_PATH_FORMAT = "%s/jobs/%d/%s/%d";
    private static final String ETL_OUTPUT_FILE_NAME_DESC = "label.tableId.partitionId.indexId.bucket.schemaHash.parquet";
    // tableId.partitionId.indexId.bucket.schemaHash
    public static final String ETL_OUTPUT_FILE_NAME_NO_LABEL_SUFFIX_FORMAT = "%d.%d.%d.%d.%d";
    public static final String ETL_OUTPUT_FILE_FORMAT = "parquet";

    // dpp result
    public static final String DPP_RESULT_NAME = "dpp_result.json";

    public Map<Long, EtlTable> tables;
    public String outputPath;
    public String outputFilePattern;
    public String label;
    public EtlJobProperty properties;
    // private EtlErrorHubInfo hubInfo;

    public EtlJobConfig(Map<Long, EtlTable> tables, String outputFilePattern, String label, EtlJobProperty properties) {
        this.tables = tables;
        // set outputPath when submit etl job
        this.outputPath = null;
        this.outputFilePattern = outputFilePattern;
        this.label = label;
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "EtlJobConfig{" +
                "tables=" + tables +
                ", outputPath='" + outputPath + '\'' +
                ", outputFilePattern='" + outputFilePattern + '\'' +
                ", label='" + label + '\'' +
                ", properties=" + properties +
                '}';
    }

	public String getOutputPath() {
        return outputPath;
    }

    public static String getOutputPath(String hdfsEtlPath, long dbId, String loadLabel, long taskSignature) {
        return String.format(ETL_OUTPUT_PATH_FORMAT, hdfsEtlPath, dbId, loadLabel, taskSignature);
    }

    public static String getDppResultFilePath(String outputPath) {
        return outputPath + "/" + DPP_RESULT_NAME;
    }

    public static String getTabletMetaStr(String filePath) throws Exception {
        // ETL_OUTPUT_FILE_NAME_DESC
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String[] fileNameArr = fileName.split("\\.");
        if (fileNameArr.length != ETL_OUTPUT_FILE_NAME_DESC.split("\\.").length) {
            throw new Exception("etl output file name error, format: " + ETL_OUTPUT_FILE_NAME_DESC
                                        + ", name: " + fileName);
        }

        // tableId.partitionId.indexId.bucket.schemaHash
        return fileName.substring(fileName.indexOf(".") + 1, fileName.lastIndexOf("."));
    }

    public static class EtlJobProperty implements Serializable {
        public boolean strictMode;
        public String timezone;

        @Override
        public String toString() {
            return "EtlJobProperty{" +
                    "strictMode=" + strictMode +
                    ", timezone='" + timezone + '\'' +
                    '}';
        }
    }

    public static class EtlTable implements Serializable {
        public List<EtlIndex> indexes;
        public EtlPartitionInfo partitionInfo;
        public List<EtlFileGroup> fileGroups;

        public EtlTable(List<EtlIndex> etlIndexes, EtlPartitionInfo etlPartitionInfo) {
            this.indexes = etlIndexes;
            this.partitionInfo = etlPartitionInfo;
            this.fileGroups = Lists.newArrayList();
        }

        public void addFileGroup(EtlFileGroup etlFileGroup) {
            fileGroups.add(etlFileGroup);
        }

        @Override
        public String toString() {
            return "EtlTable{" +
                    "indexes=" + indexes +
                    ", partitionInfo=" + partitionInfo +
                    ", fileGroups=" + fileGroups +
                    '}';
        }
    }

    public static class EtlColumn implements Serializable {
        public String columnName;
        public String columnType;
        public boolean isAllowNull;
        public boolean isKey;
        public String aggregationType;
        public String defaultValue;
        public int stringLength;
        public int precision;
        public int scale;

        public EtlColumn(String columnName, String columnType, boolean isAllowNull, boolean isKey,
                         String aggregationType, String defaultValue, int stringLength, int precision, int scale) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.isAllowNull = isAllowNull;
            this.isKey = isKey;
            this.aggregationType = aggregationType;
            this.defaultValue = defaultValue;
            this.stringLength = stringLength;
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public String toString() {
            return "EtlColumn{" +
                    "columnName='" + columnName + '\'' +
                    ", columnType='" + columnType + '\'' +
                    ", isAllowNull=" + isAllowNull +
                    ", isKey=" + isKey +
                    ", aggregationType='" + aggregationType + '\'' +
                    ", defaultValue='" + defaultValue + '\'' +
                    ", stringLength=" + stringLength +
                    ", precision=" + precision +
                    ", scale=" + scale +
                    '}';
        }
    }

    public static class EtlIndexComparator implements Comparator<EtlIndex> {
        @Override
        public int compare(EtlIndex a, EtlIndex b) {
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

    public static class EtlIndex implements Serializable {
        public long indexId;
        public List<EtlColumn> columns;
        public int schemaHash;
        public String indexType;
        public boolean isBaseIndex;

        public EtlIndex(long indexId, List<EtlColumn> etlColumns, int schemaHash,
                        String indexType, boolean isBaseIndex) {
            this.indexId = indexId;
            this.columns =  etlColumns;
            this.schemaHash = schemaHash;
            this.indexType = indexType;
            this.isBaseIndex = isBaseIndex;
        }

        public EtlColumn getColumn(String name) {
            for (EtlColumn column : columns) {
                if (column.columnName.equals(name)) {
                    return column;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "EtlIndex{" +
                    "indexId=" + indexId +
                    ", columns=" + columns +
                    ", schemaHash=" + schemaHash +
                    ", indexType='" + indexType + '\'' +
                    ", isBaseIndex=" + isBaseIndex +
                    '}';
        }
    }

    public static class EtlPartitionInfo implements Serializable {
        public String partitionType;
        public List<String> partitionColumnRefs;
        public List<String> distributionColumnRefs;
        public List<EtlPartition> partitions;

        public EtlPartitionInfo(String partitionType, List<String> partitionColumnRefs,
                                List<String> distributionColumnRefs, List<EtlPartition> etlPartitions) {
            this.partitionType = partitionType;
            this.partitionColumnRefs = partitionColumnRefs;
            this.distributionColumnRefs = distributionColumnRefs;
            this.partitions = etlPartitions;
        }

        @Override
        public String toString() {
            return "EtlPartitionInfo{" +
                    "partitionType='" + partitionType + '\'' +
                    ", partitionColumnRefs=" + partitionColumnRefs +
                    ", distributionColumnRefs=" + distributionColumnRefs +
                    ", partitions=" + partitions +
                    '}';
        }
    }

    public static class EtlPartition implements Serializable {
        public long partitionId;
        public List<Object> startKeys;
        public List<Object> endKeys;
        public boolean isMaxPartition;
        public int bucketNum;

        public EtlPartition(long partitionId, List<Object> startKeys, List<Object> endKeys,
                            boolean isMaxPartition, int bucketNum) {
            this.partitionId = partitionId;
            this.startKeys = startKeys;
            this.endKeys = endKeys;
            this.isMaxPartition = isMaxPartition;
            this.bucketNum = bucketNum;
        }

        @Override
        public String toString() {
            return "EtlPartition{" +
                    "partitionId=" + partitionId +
                    ", startKeys=" + startKeys +
                    ", endKeys=" + endKeys +
                    ", isMaxPartition=" + isMaxPartition +
                    ", bucketNum=" + bucketNum +
                    '}';
        }
    }

    public static class EtlFileGroup implements Serializable {
        public List<String> filePaths;
        public List<String> fileFieldNames;
        public List<String> columnsFromPath;
        public String columnSeparator;
        public String lineDelimiter;
        public boolean isNegative;
        public String fileFormat;
        public Map<String, EtlColumnMapping> columnMappings;
        public String where;
        public List<Long> partitions;
        public String hiveTableName;

        public EtlFileGroup(List<String> filePaths, List<String> fileFieldNames, List<String> columnsFromPath,
                            String columnSeparator, String lineDelimiter, boolean isNegative, String fileFormat,
                            Map<String, EtlColumnMapping> columnMappings, String where, List<Long> partitions) {
            this.filePaths = filePaths;
            this.fileFieldNames = fileFieldNames;
            this.columnsFromPath = columnsFromPath;
            this.columnSeparator = columnSeparator;
            this.lineDelimiter = lineDelimiter;
            this.isNegative = isNegative;
            this.fileFormat = fileFormat;
            this.columnMappings = columnMappings;
            this.where = where;
            this.partitions = partitions;
        }

        @Override
        public String toString() {
            return "EtlFileGroup{" +
                    "filePaths=" + filePaths +
                    ", fileFieldNames=" + fileFieldNames +
                    ", columnsFromPath=" + columnsFromPath +
                    ", columnSeparator='" + columnSeparator + '\'' +
                    ", lineDelimiter='" + lineDelimiter + '\'' +
                    ", isNegative=" + isNegative +
                    ", fileFormat='" + fileFormat + '\'' +
                    ", columnMappings=" + columnMappings +
                    ", where='" + where + '\'' +
                    ", partitions=" + partitions +
                    ", hiveTableName='" + hiveTableName + '\'' +
                    '}';
        }
    }

    public static class EtlColumnMapping implements Serializable {
        public String functionName;
        public List<String> args;
        public String expr;
        public Map<String, String> functionMap =
                new ImmutableMap.Builder<String, String>().
                        put("md5sum", "md5").build();


        public EtlColumnMapping(String functionName, List<String> args) {
            this.functionName = functionName;
            this.args = args;
        }

        public EtlColumnMapping(String expr) {
            this.expr = expr;
        }

        public String toDescription() {
            StringBuilder sb = new StringBuilder();
            if (functionName == null) {
                sb.append(expr);
            } else {
                if (functionMap.containsKey(functionName)) {
                    sb.append(functionMap.get(functionName));
                } else {
                    sb.append(functionName);
                }
                sb.append("(");
                if (args != null) {
                    for (String arg : args) {
                        sb.append(arg);
                        sb.append(",");
                    }
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return "EtlColumnMapping{" +
                    "functionName='" + functionName + '\'' +
                    ", args=" + args +
                    ", expr=" + expr +
                    '}';
        }
    }
}