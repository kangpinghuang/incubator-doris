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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partitioner;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import scala.Tuple2;
import scala.collection.Seq;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.Collections;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.zip.CRC32;

import org.apache.hadoop.fs.FileSystem;
import scala.collection.JavaConverters;

// This class is a Spark-based data preprocessing program,
// which will make use of the distributed compute framework of spark to
// do ETL job/sort/preaggregate/convert data format job in spark job
// to boost the process of large amount of data load.
public final class SparkDpp implements java.io.Serializable {

    private static final Logger LOG = LogManager.getLogger(SparkDpp.class);
    private JobConf jobConf;
    private static final String BUCKET_ID = "__bucketId__";

    public boolean parseConf(String jsonConf) {
        Gson gson = new Gson();
        try {
            jobConf = gson.fromJson(jsonConf, JobConf.class);
        } catch (JsonSyntaxException jsonException) {
            System.out.println("Parse job config failed, error:" + jsonException.toString());
            return false;
        }
        return true;
    }

    public String jobLabel() {
        return jobConf.label;
    }

    private Class dataTypeToClass(DataType dataType) {
        if (dataType.equals(DataTypes.BooleanType)) {
            return Boolean.class;
        } else if (dataType.equals(DataTypes.ShortType)) {
            return Short.class;
        } else if (dataType.equals(DataTypes.IntegerType)) {
            return Integer.class;
        } else if (dataType.equals(DataTypes.LongType)) {
            return Long.class;
        } else if (dataType.equals(DataTypes.FloatType)) {
            return Float.class;
        } else if (dataType.equals(DataTypes.DoubleType)) {
            return Double.class;
        } else if (dataType.equals(DataTypes.DateType)) {
            return Date.class;
        } else if (dataType.equals(DataTypes.StringType)) {
            return String.class;
        } else if (dataType.equals(DataTypes.ShortType)) {
            return Short.class;
        }
        return null;
    }

    private DataType columnTypeToDataType(ColumnType columnType) {
        DataType structColumnType = DataTypes.StringType;
        switch (columnType) {
            case BOOL:
                structColumnType = DataTypes.BooleanType;
                break;
            case TINYINT:
            case SMALLINT:
                structColumnType = DataTypes.ShortType;
                break;
            case INT:
                structColumnType = DataTypes.IntegerType;
                break;
            case DATETIME:
            case BIGINT:
            case LARGEINT:
                // todo: special treat LARGEINT because spark not support int128
                structColumnType = DataTypes.LongType;
                break;
            case FLOAT:
                structColumnType = DataTypes.FloatType;
                break;
            case DOUBLE:
                structColumnType = DataTypes.DoubleType;
                break;
            case DATE:
                structColumnType = DataTypes.DateType;
                break;
            case HLL:
            case CHAR:
            case VARCHAR:
            case BITMAP:
            case OBJECT:
                structColumnType = DataTypes.StringType;
                break;
            default:
                System.out.println("invalid column type:" + columnType);
                break;
        }
        return structColumnType;
    }

    private ByteBuffer getHashValue(Object o, DataType type) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (type.equals(DataTypes.ShortType)) {
            buffer.putShort((Short)o);
        } else if (type.equals(DataTypes.IntegerType)) {
            buffer.putInt((Integer) o);
        } else if (type.equals(DataTypes.LongType)) {
            buffer.putLong((Long)o);
        } else if (type.equals(DataTypes.StringType)) {
            try {
                String str = (String)o;
                buffer = ByteBuffer.wrap(str.getBytes("UTF-8"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return buffer;
    }

    private StructType createDstTableSchema(List<IndexMeta.ColumnDescription> columns, boolean addBucketIdColumn) {
        List<StructField> fields = new ArrayList<>();
        if (addBucketIdColumn) {
            StructField bucketIdField = DataTypes.createStructField(BUCKET_ID, DataTypes.StringType, true);
            fields.add(bucketIdField);
        }
        for (IndexMeta.ColumnDescription columnDescription : columns) {
            // user StringType to load source data
            // TODO: process null "\N"
            String columnName = columnDescription.columnName;
            ColumnType columnType = columnDescription.columnType;
            DataType structColumnType = columnTypeToDataType(columnType);
            StructField field = DataTypes.createStructField(columnName, structColumnType, columnDescription.isAllowNull);
            fields.add(field);
        }
        StructType dstSchema = DataTypes.createStructType(fields);
        return dstSchema;
    }

    private Dataset<Row> processDataframeAgg(Dataset<Row> dataframe, IndexMeta indexMeta) {
        List<Column> keyColumns = new ArrayList<>();
        // add BUCKET_ID as a Key
        keyColumns.add(new Column(BUCKET_ID));
        Map<String, String> aggFieldOperations = new HashMap<>();
        Map<String, String> renameMap = new HashMap<>();
        for (IndexMeta.ColumnDescription columnDescription : indexMeta.columns) {
            if (columnDescription.isKey) {
                keyColumns.add(new Column(columnDescription.columnName));
                continue;
            }
            if (columnDescription.aggregationType == AggregationType.MAX) {
                aggFieldOperations.put(columnDescription.columnName, "max");
                renameMap.put(columnDescription.columnName, "max(" + columnDescription.columnName + ")");
            } else if (columnDescription.aggregationType == AggregationType.MIN) {
                aggFieldOperations.put(columnDescription.columnName, "min");
                renameMap.put(columnDescription.columnName, "min(" + columnDescription.columnName + ")");
            } else if (columnDescription.aggregationType == AggregationType.SUM) {
                aggFieldOperations.put(columnDescription.columnName, "sum");
                renameMap.put(columnDescription.columnName, "sum(" + columnDescription.columnName + ")");
            } else if (columnDescription.aggregationType == AggregationType.HLL_UNION) {
                aggFieldOperations.put(columnDescription.columnName, "hll_union");
                renameMap.put(columnDescription.columnName, "hll_union(" + columnDescription.columnName + ")");
            } else if (columnDescription.aggregationType == AggregationType.BITMAP_UNION) {
                aggFieldOperations.put(columnDescription.columnName, "bitmap_union");
                renameMap.put(columnDescription.columnName, "bitmap_union(" + columnDescription.columnName + ")");
            } else if (columnDescription.aggregationType == AggregationType.REPLACE) {
                aggFieldOperations.put(columnDescription.columnName, "replace");
                renameMap.put(columnDescription.columnName, "replace(" + columnDescription.columnName + ")");
            } else if (columnDescription.aggregationType == AggregationType.REPLACE_IF_NOT_NULL) {
                aggFieldOperations.put(columnDescription.columnName, "replace_if_not_null");
                renameMap.put(columnDescription.columnName, "replace_if_not_null(" + columnDescription.columnName + ")");
            } else {
                System.out.println("INVALID aggregation type:" + columnDescription.aggregationType);
            }
        }
        Seq<Column> keyColumnSeq = JavaConverters.asScalaIteratorConverter(keyColumns.iterator()).asScala().toSeq();
        Dataset<Row> aggDataFrame = dataframe.groupBy(keyColumnSeq).agg(aggFieldOperations);
        for (Map.Entry<String, String> renameItem : renameMap.entrySet()) {
            aggDataFrame = aggDataFrame.withColumnRenamed(renameItem.getValue(), renameItem.getKey());
        }
        return aggDataFrame;
    }

    // The dataframe is partitioned by bucketKey, and sorted by bucketKey and key columns.
    // This function will write each bucket data to file within partition
    // Should extend this logic for different output format
    private void writePartitionedAndSortedDataframeToFile(Dataset<Row> dataframe, String pathPattern,
                                                          int tableId, int curIndexId, int schemaHash) {
        dataframe.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                // write the data to dst file
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(URI.create(jobConf.output_path), conf);
                String lastBucketKey = null;
                BufferedWriter writer = null;
                while (t.hasNext()) {
                    Row row = t.next();
                    if (row.length() <= 1) {
                        System.out.println("invalid row:" + row);
                        continue;
                    }
                    String curBucketKey = row.getString(0);
                    StringBuilder sb = new StringBuilder();
                    // now just write in csv format
                    for (int i = 1; i < row.length(); ++i) {
                        sb.append(row.get(i).toString());
                        if (i == row.length() - 1) {
                            break;
                        }
                        // ok, use ',' as seperator
                        sb.append(",");
                    }
                    if (lastBucketKey == null || !curBucketKey.equals(lastBucketKey)) {
                        if (writer != null) {
                            System.out.println("close writer");
                            writer.close();
                        }
                        // flush current writer and create a new writer
                        String[] bucketKey = curBucketKey.split("_");
                        if (bucketKey.length != 2) {
                            System.out.println("invalid bucket key:" + curBucketKey);
                            continue;
                        }
                        int partitionId = Integer.parseInt(bucketKey[0]);
                        int bucketId = Integer.parseInt(bucketKey[1]);
                        String path = String.format(pathPattern, tableId, partitionId, curIndexId, bucketId, schemaHash);
                        writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
                        if(writer != null){
                            System.out.println("[HdfsOperate]>> initialize writer succeed! path:" + path);
                        }
                        lastBucketKey = curBucketKey;
                    }
                    writer.write(sb.toString() + "\n");
                }
                if (writer != null) {
                    writer.close();
                }
            }
        });
    }

    private void processRollupTree(RollupTreeNode rootNode,
                                   Dataset<Row> rootDataframe, int tableId, TableMeta tableMeta, IndexMeta baseIndex) {
        Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
        nodeQueue.offer(rootNode);
        int currentLevel = 0;
        // level travel the tree
        Map<Integer, Dataset<Row>> parentDataframeMap = new HashMap<>();
        parentDataframeMap.put(baseIndex.indexId, rootDataframe);
        Map<Integer, Dataset<Row>> childrenDataframeMap = new HashMap<>();
        String pathPattern = jobConf.output_path + "/" + jobConf.output_file_pattern;
        while (!nodeQueue.isEmpty()) {
            RollupTreeNode curNode = nodeQueue.poll();
            if (curNode.children != null) {
                for (RollupTreeNode child : curNode.children) {
                    nodeQueue.offer(child);
                }
            }
            Dataset<Row> curDataFrame = null;
            // column select for rollup
            if (curNode.level != currentLevel) {
                // need to unpersist parent dataframe
                currentLevel = curNode.level;
                parentDataframeMap.clear();
                parentDataframeMap = childrenDataframeMap;
                childrenDataframeMap = new HashMap<>();
            }
            // TODO: check here
            int parentIndexId = baseIndex.indexId;
            if (curNode.parent != null) {
                parentIndexId = curNode.parent.indexId;
            }

            Dataset<Row> parentDataframe = parentDataframeMap.get(parentIndexId);
            List<Column> columns = new ArrayList<>();
            List<Column> keyColumns = new ArrayList<>();
            Column bucketIdColumn = new Column(BUCKET_ID);
            keyColumns.add(bucketIdColumn);
            columns.add(bucketIdColumn);
            for (String keyName : curNode.keyColumnNames) {
                columns.add(new Column(keyName));
                keyColumns.add(new Column(keyName));
            }
            for (String valueName : curNode.valueColumnNames) {
                columns.add(new Column(valueName));
            }
            Seq<Column> columnSeq = JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq();
            curDataFrame = parentDataframe.select(columnSeq);

            if (curNode.indexMeta.indexType == IndexType.AGGREGATE) {
                // do aggregation
                curDataFrame = processDataframeAgg(curDataFrame, curNode.indexMeta);
            }
            Seq<Column> keyColumnSeq = JavaConverters.asScalaIteratorConverter(keyColumns.iterator()).asScala().toSeq();
            // should use sortWithinPartitions, not sort
            // because sort will modify the partition number which will lead to bug
            curDataFrame = curDataFrame.sortWithinPartitions(keyColumnSeq);
            childrenDataframeMap.put(curNode.indexId, curDataFrame);

            if (curNode.children != null && curNode.children.size() > 1) {
                // if the children number larger than 1, persist the dataframe for performance
                curDataFrame.persist();
            }
            int curIndexId = curNode.indexId;
            writePartitionedAndSortedDataframeToFile(curDataFrame, pathPattern, tableId, curIndexId, curNode.indexMeta.schemaHash);
        }
    }

    private long getHashValue(Row row, List<String> distributeColumns, StructType dstTableSchema) {
        CRC32 hashValue = new CRC32();
        for (String distColumn : distributeColumns) {
            Object columnObject = row.get(row.fieldIndex(distColumn));
            ByteBuffer buffer = getHashValue(columnObject, dstTableSchema.apply(distColumn).dataType());
            hashValue.update(buffer.array(), 0, buffer.limit());
        }
        return hashValue.getValue();
    }

    private Dataset<Row> repartitionDataframeByBucketId(SparkSession spark, Dataset<Row> dataframe,
                                                        PartitionInfo partitionInfo, List<Integer> partitionKeyIndex,
                                                        List<Class> partitionKeySchema,
                                                        List<DppColumns> partitionRangeKeys,
                                                        List<String> keyColumnNames,
                                                        List<String> valueColumnNames,
                                                        StructType dstTableSchema,
                                                        IndexMeta baseIndex) {
        List<String> distributeColumns = partitionInfo.distributionColumnRefs;
        Partitioner partitioner = new DorisRangePartitioner(partitionInfo, partitionKeyIndex, partitionRangeKeys);

        JavaPairRDD<String, DppColumns> pairRDD = dataframe.javaRDD().mapToPair(new PairFunction<Row, String, DppColumns>() {
            @Override
            public Tuple2<String, DppColumns> call(Row row) throws Exception {
                List<Object> columns = new ArrayList<>();
                List<Class> classes = new ArrayList<>();
                List<Object> keyColumns = new ArrayList<>();
                List<Class> keyClasses = new ArrayList<>();
                for (String columnName : keyColumnNames) {
                    Object columnObject = row.get(row.fieldIndex(columnName));
                    columns.add(columnObject);
                    classes.add(dataTypeToClass(dstTableSchema.apply(dstTableSchema.fieldIndex(columnName)).dataType()));
                    keyColumns.add(columnObject);
                    keyClasses.add(dataTypeToClass(dstTableSchema.apply(dstTableSchema.fieldIndex(columnName)).dataType()));
                }

                for (String columnName : valueColumnNames) {
                    columns.add(row.get(row.fieldIndex(columnName)));
                    classes.add(dataTypeToClass(dstTableSchema.apply(dstTableSchema.fieldIndex(columnName)).dataType()));
                }
                DppColumns dppColumns = new DppColumns(columns, classes);
                // TODO: judge invalid partitionId
                DppColumns key = new DppColumns(keyColumns, keyClasses);
                int pid = partitioner.getPartition(key);
                if (pid < 0) {
                    // TODO: add log for invalid partition id
                }
                System.out.println("partitionInfo.partitions:" + partitionInfo.partitions + ", pid:" + pid);
                long hashValue = getHashValue(row, distributeColumns, dstTableSchema);
                System.out.println("partitionInfo.partitions.get(pid).bucketsNum:" + partitionInfo.partitions.get(pid).bucketNum + ", hashValue:" + hashValue);
                int bucketId = (int) ((hashValue & 0xffffffff) % partitionInfo.partitions.get(pid).bucketNum);
                int partitionId = partitionInfo.partitions.get(pid).partitionId;
                // bucketKey is partitionId_bucketId
                String bucketKey = Integer.toString(partitionId) + "_" + Integer.toString(bucketId);
                return new Tuple2<String, DppColumns>(bucketKey, dppColumns);
            }}
        );

        JavaRDD<Row> resultRdd = pairRDD.map( record -> {
                    String bucketKey = record._1;
                    List<Object> row = new ArrayList<>();
                    // bucketKey as the first key
                    row.add(bucketKey);
                    row.addAll(record._2.columns);
                    return RowFactory.create(row.toArray());
                }
        );

        StructType midTableSchema = createDstTableSchema(baseIndex.columns, true);
        dataframe = spark.createDataFrame(resultRdd, midTableSchema);
        dataframe = dataframe.repartition(partitionInfo.partitions.size(), new Column(BUCKET_ID));
        return dataframe;
    }

    private Dataset<Row> convertSrcDataframeToDstDataframe(Dataset<Row> srcDataframe,
                                                           StructType srcSchema,
                                                           StructType dstTableSchema) {
        // TODO: add mapping columns and default value columns and negative load
        // assume that all columns is simple mapping
        // TODO: calculate the mapping columns
        // TODO: exception handling
        Dataset<Row> dataframe = srcDataframe;
        for (StructField field : srcSchema.fields()) {
            StructField dstField = dstTableSchema.apply(field.name());
            if (!dstField.dataType().equals(field.dataType())) {
                dataframe = dataframe.withColumn(field.name(), dataframe.col(field.name()).cast(dstField.dataType()));
            }
        }
        return dataframe;
    }

    private Dataset<Row> loadDataFromPath(SparkSession spark,
                                          FileGroup fileGroup,
                                          String fileUrl,
                                          StructType srcSchema,
                                          List<String> dataSrcColumns) {
        JavaRDD<String> sourceDataRdd = spark.read().textFile(fileUrl).toJavaRDD();
        JavaRDD<Row> rowRDD = sourceDataRdd.map((Function<String, Row>) record -> {
            // TODO: process null value
            String[] attributes = record.split(fileGroup.columnSeparator);
            if (attributes.length != dataSrcColumns.size()) {
                // update invalid row counter statistics
                // this counter will be record in result.json
            }
            return RowFactory.create(attributes);
        });

        Dataset<Row> dataframe = spark.createDataFrame(rowRDD, srcSchema);
        return dataframe;
    }

    private StructType createScrSchema(List<String> srcColumns) {
        List<StructField> fields = new ArrayList<>();
        for (String srcColumn : srcColumns) {
            // user StringType to load source data
            StructField field = DataTypes.createStructField(srcColumn, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType srcSchema = DataTypes.createStructType(fields);
        return srcSchema;
    }

    private List<DppColumns> createPartitionRangeKeys(PartitionInfo partitionInfo, List<Class> partitionKeySchema) {
        List<DppColumns> partitionRangeKeys = new ArrayList<>();
        for (PartitionInfo.PartitionDescription partitionDescription : partitionInfo.partitions) {
            List<Object> keys = new ArrayList<>();
            for (Short value : partitionDescription.startKeys) {
                keys.add(value);
                System.out.println("add partition:" + value);
            }
            partitionRangeKeys.add(new DppColumns(keys, partitionKeySchema));
            if (partitionDescription.isMaxPartition) {
                List<Object> endKeys = new ArrayList<>();
                //List<Class> endKeysClasses = new ArrayList<>();
                for (Short value : partitionDescription.endKeys) {
                    endKeys.add(value);
                    System.out.println("add partition:" + value);
                }
                partitionRangeKeys.add(new DppColumns(endKeys, partitionKeySchema));
            }
        }
        // sort the partition range keys to make sure it is asc order
        Collections.sort(partitionRangeKeys);
        System.out.println("partitionRangeKeys:" + partitionRangeKeys);
        return partitionRangeKeys;
    }

    public void doDpp() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName(jobLabel())
                .getOrCreate();

        for (Map.Entry<Integer, TableMeta> entry : jobConf.tables.entrySet()) {
            Integer tableId = entry.getKey();
            TableMeta tableMeta = entry.getValue();

            // get the base index meta
            IndexMeta baseIndex = null;
            for (IndexMeta indexMeta : tableMeta.indexes) {
                if (indexMeta.isBaseIndex) {
                    baseIndex = indexMeta;
                    break;
                }
            }

            // get key column names and value column names seperately
            List<String> keyColumnNames = new ArrayList<>();
            List<String> valueColumnNames = new ArrayList<>();
            for (IndexMeta.ColumnDescription indexColumnDescription : baseIndex.columns) {
                if (indexColumnDescription.isKey) {
                    keyColumnNames.add(indexColumnDescription.columnName);
                } else {
                    valueColumnNames.add(indexColumnDescription.columnName);
                }
            }

            PartitionInfo partitionInfo = tableMeta.partitionInfo;
            List<Integer> partitionKeyIndex = new ArrayList<Integer>();
            List<Class> partitionKeySchema = new ArrayList<>();
            for (String key : partitionInfo.partitionColumnNames) {
                for (int i = 0; i < baseIndex.columns.size(); ++i) {
                    IndexMeta.ColumnDescription columnDescription = baseIndex.columns.get(i);
                    if (columnDescription.columnName.equals(key)) {
                        partitionKeyIndex.add(i);
                        partitionKeySchema.add(dataTypeToClass(columnTypeToDataType(columnDescription.columnType)));
                        break;
                    }
                }
            }
            List<DppColumns> partitionRangeKeys = createPartitionRangeKeys(partitionInfo, partitionKeySchema);
            LOG.info("partitionRangeKeys:" + partitionRangeKeys);
            StructType dstTableSchema = createDstTableSchema(baseIndex.columns, false);
            RollupTreeBuilder rollupTreeParser = new MinimalCoverRollupTreeBuilder();
            RollupTreeNode rootNode = rollupTreeParser.build(tableMeta);

            for (FileGroup fileGroup : tableMeta.fileGroups) {
                // TODO: process the case the user do not provide the source schema
                List<String> dataSrcColumns = fileGroup.columns;
                StructType srcSchema = createScrSchema(dataSrcColumns);
                List<String> filePaths = fileGroup.filePaths;
                for (String filePath : filePaths) {
                    if (fileGroup.columnSeparator == null) {
                        System.out.println("invalid null column separator!");
                        System.exit(-1);
                    }
                    Dataset<Row> dataframe = loadDataFromPath(spark, fileGroup, filePath, srcSchema, dataSrcColumns);
                    dataframe = convertSrcDataframeToDstDataframe(dataframe, srcSchema,dstTableSchema);

                    if (!fileGroup.where.isEmpty()) {
                        dataframe = dataframe.filter(fileGroup.where);
                    }

                    dataframe = repartitionDataframeByBucketId(spark, dataframe,
                            partitionInfo, partitionKeyIndex,
                            partitionKeySchema, partitionRangeKeys,
                            keyColumnNames, valueColumnNames,
                            dstTableSchema, baseIndex);

                    processRollupTree(rootNode, dataframe, tableId, tableMeta, baseIndex);
                }
            }
        }
        spark.stop();
    }
}
