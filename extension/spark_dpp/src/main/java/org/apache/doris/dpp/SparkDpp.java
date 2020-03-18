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
import org.apache.spark.api.java.function.MapFunction;
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
            System.err.println("Parse job config failed, error:" + jsonException.toString());
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
                System.err.println("invalid column type:" + columnType);
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

    private StructType createDstTableSchema(List<ColumnMeta> columns, boolean addBucketIdColumn) {
        List<StructField> fields = new ArrayList<>();
        if (addBucketIdColumn) {
            StructField bucketIdField = DataTypes.createStructField(BUCKET_ID, DataTypes.StringType, true);
            fields.add(bucketIdField);
        }
        for (ColumnMeta columnMeta : columns) {
            // user StringType to load source data
            // TODO: process null "\N"
            String columnName = columnMeta.columnName;
            System.err.println("column name:" + columnName);
            ColumnType columnType = columnMeta.columnType;
            DataType structColumnType = columnTypeToDataType(columnType);
            StructField field = DataTypes.createStructField(columnName, structColumnType, columnMeta.isAllowNull);
            fields.add(field);
        }
        StructType dstSchema = DataTypes.createStructType(fields);
        return dstSchema;
    }

    private Dataset<Row> processDataframeAgg(Dataset<Row> dataframe, IndexMeta indexMeta) {
        List<Column> keyColumns = new ArrayList<>();
        keyColumns.add(new Column(BUCKET_ID));
        Map<String, String> aggFieldOperations = new HashMap<>();
        Map<String, String> renameMap = new HashMap<>();
        for (IndexMeta.IndexColumnDescription columnDescription : indexMeta.columnDescriptions) {
            if (columnDescription.isKey) {
                keyColumns.add(new Column(columnDescription.name));
                continue;
            }
            if (columnDescription.aggregationType == AggregationType.MAX) {
                aggFieldOperations.put(columnDescription.name, "max");
                renameMap.put(columnDescription.name, "max(" + columnDescription.name + ")");
            } else if (columnDescription.aggregationType == AggregationType.MIN) {
                aggFieldOperations.put(columnDescription.name, "min");
                renameMap.put(columnDescription.name, "min(" + columnDescription.name + ")");
            } else if (columnDescription.aggregationType == AggregationType.SUM) {
                aggFieldOperations.put(columnDescription.name, "sum");
                renameMap.put(columnDescription.name, "sum(" + columnDescription.name + ")");
            } else if (columnDescription.aggregationType == AggregationType.HLL_UNION) {
                aggFieldOperations.put(columnDescription.name, "hll_union");
                renameMap.put(columnDescription.name, "hll_union(" + columnDescription.name + ")");
            } else if (columnDescription.aggregationType == AggregationType.BITMAP_UNION) {
                aggFieldOperations.put(columnDescription.name, "bitmap_union");
                renameMap.put(columnDescription.name, "bitmap_union(" + columnDescription.name + ")");
            } else if (columnDescription.aggregationType == AggregationType.REPLACE) {
                aggFieldOperations.put(columnDescription.name, "replace");
                renameMap.put(columnDescription.name, "replace(" + columnDescription.name + ")");
            } else if (columnDescription.aggregationType == AggregationType.REPLACE_IF_NOT_NULL) {
                aggFieldOperations.put(columnDescription.name, "replace_if_not_null");
                renameMap.put(columnDescription.name, "replace_if_not_null(" + columnDescription.name + ")");
            } else {
                System.err.println("INVALID aggregation type:" + columnDescription.aggregationType);
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
    private void writePartitionedAndSortedDataframeToFile(Dataset<Row> dataframe, String pathPrefix, int curIndexId) {
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
                        System.err.println("invalid row:" + row);
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
                    if (lastBucketKey == null && !curBucketKey.equals(lastBucketKey)) {
                        if (writer != null) {
                            System.err.println("close writer");
                            writer.close();
                        }
                        // flush current writer and create a new writer
                        String path = pathPrefix + "/" + curIndexId + "_" + curBucketKey;
                        writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
                        System.err.println("new writer for path:" + path);
                        if(writer != null){
                            System.err.println("[HdfsOperate]>> initialize writer succeed! path:" + path);
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

    private void processRollupTree(RollupTreeNode rootNode, Dataset<Row> rootDataframe, TableMeta tableMeta, IndexMeta baseIndex) {
        Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
        nodeQueue.offer(rootNode);
        int currentLevel = 0;
        // level travel the tree
        Map<Integer, Dataset<Row>> parentDataframeMap = new HashMap<>();
        parentDataframeMap.put(baseIndex.indexId, rootDataframe);
        Map<Integer, Dataset<Row>> childrenDataframeMap = new HashMap<>();
        String pathPrefix = jobConf.output_path + "/" + jobLabel() + "/" + System.currentTimeMillis();
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
            int parentIndexId = tableMeta.baseIndex;
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
            System.out.println("319, cur node id:" + curNode.indexId + ", parent id:" + parentIndexId);
            parentDataframe.show();
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
            System.out.println("336, curNode index:" + curIndexId);
            curDataFrame.show();
            System.out.println("338, curNode path prefix:" + pathPrefix);
            writePartitionedAndSortedDataframeToFile(curDataFrame, pathPrefix, curIndexId);
        }
    }

    private int processRollupTreeNode(RollupTreeNode curNode, int currentLevel,
                                       TableMeta tableMeta, String pathPrefix,
                                       Map<Integer, Dataset<Row>> parentDataframeMap,
                                       Map<Integer, Dataset<Row>> childrenDataframeMap) {
        Dataset<Row> curDataFrame = null;
        // column select for rollup
        if (curNode.level != currentLevel) {
            // need to unpersist parent dataframe
            currentLevel = curNode.level;
            parentDataframeMap.clear();
            parentDataframeMap = childrenDataframeMap;
            childrenDataframeMap = new HashMap<>();
        }
        int parentIndexId = tableMeta.baseIndex;
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
        System.out.println("319, cur node id:" + curNode.indexId + ", parent id:" + parentIndexId);
        parentDataframe.show();
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
        System.out.println("336, curNode index:" + curIndexId);
        curDataFrame.show();
        System.out.println("338, curNode path prefix:" + pathPrefix);
        writePartitionedAndSortedDataframeToFile(curDataFrame, pathPrefix, curIndexId);
        return currentLevel;
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
                                                        TableMeta tableMeta) {
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
                int partitionId = partitioner.getPartition(key);
                if (partitionId < 0) {
                    // TODO: add log for invalid partition id
                }
                int bucketId = (int) ((getHashValue(row, distributeColumns, dstTableSchema) & 0xffffffff)
                        % partitionInfo.partitions.get(partitionId).bucketsNum);
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

        StructType midTableSchema = createDstTableSchema(tableMeta.columns, true);
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
                                          SourceDescription source,
                                          String fileUrl,
                                          StructType srcSchema,
                                          List<String> dataSrcColumns) {
        JavaRDD<String> sourceDataRdd = spark.read().textFile(fileUrl).toJavaRDD();
        JavaRDD<Row> rowRDD = sourceDataRdd.map((Function<String, Row>) record -> {
            // replace with source.column_seperator
            // TODO: process null value
            //String[] attributes = record.split(source.columnSeperator);
            String[] attributes = record.split("\t");
            if (attributes.length != dataSrcColumns.size()) {
                // update invalid row counter statistics
                // this counter will be record in result.json
            }
            return RowFactory.create(attributes);
        });

        Dataset<Row> dataframe = spark.createDataFrame(rowRDD, srcSchema);
        System.out.println("out src dataframe:");
        dataframe.show();
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
            }
            partitionRangeKeys.add(new DppColumns(keys, partitionKeySchema));
            if (partitionDescription.isMaxPartition) {
                List<Object> endKeys = new ArrayList<>();
                //List<Class> endKeysClasses = new ArrayList<>();
                for (Short value : partitionDescription.endKeys) {
                    endKeys.add(value);
                }
                partitionRangeKeys.add(new DppColumns(endKeys, partitionKeySchema));
            }
        }
        Collections.sort(partitionRangeKeys);
        return partitionRangeKeys;
    }

    public void doDpp() throws Exception {
        Gson gson = new Gson();
        System.err.println("job configuration:" + gson.toJson(jobConf));

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName(jobLabel())
                .getOrCreate();

        for (Map.Entry<Integer, TableMeta> entry : jobConf.tables.entrySet()) {
            Integer tableId = entry.getKey();
            TableMeta tableMeta = entry.getValue();
            StructType dstTableSchema = createDstTableSchema(tableMeta.columns, false);
            List<Class> fieldClasses = new ArrayList<>();
            for (StructField field : dstTableSchema.fields()) {
                Class fieldClass = dataTypeToClass(field.dataType());
                fieldClasses.add(fieldClass);
            }

            // get the base index meta
            IndexMeta baseIndex = null;
            for (IndexMeta indexMeta : tableMeta.indexes) {
                if (indexMeta.indexId == tableMeta.baseIndex) {
                    baseIndex = indexMeta;
                    break;
                }
            }
            // get key column names and value column names seperately
            List<String> keyColumnNames = new ArrayList<>();
            List<String> valueColumnNames = new ArrayList<>();
            for (IndexMeta.IndexColumnDescription indexColumnDescription : baseIndex.columnDescriptions) {
                if (indexColumnDescription.isKey) {
                    keyColumnNames.add(indexColumnDescription.name);
                } else {
                    valueColumnNames.add(indexColumnDescription.name);
                }
            }

            PartitionInfo partitionInfo = tableMeta.partitionInfo;
            List<Integer> partitionKeyIndex = new ArrayList<Integer>();
            List<Class> partitionKeySchema = new ArrayList<>();
            for (String key : partitionInfo.partitionColumnNames) {
                for (ColumnMeta columnMeta : tableMeta.columns) {
                    if (columnMeta.columnName.equals(key)) {
                        partitionKeyIndex.add(columnMeta.columnId);
                        partitionKeySchema.add(dataTypeToClass(columnTypeToDataType(columnMeta.columnType)));
                        break;
                    }
                }
            }
            List<DppColumns> partitionRangeKeys = createPartitionRangeKeys(partitionInfo, partitionKeySchema);

            RollupTreeBuilder rollupTreeParser = new MinimalCoverRollupTreeBuilder();
            RollupTreeNode rootNode = rollupTreeParser.build(tableMeta);

            for (SourceDescription source : tableMeta.sources) {
                // TODO: process the case the user do not provide the source schema
                List<String> dataSrcColumns = source.columns;
                StructType srcSchema = createScrSchema(dataSrcColumns);
                List<String> fileUrls = source.fileUrls;
                for (String fileUrl : fileUrls) {
                    Dataset<Row> dataframe = loadDataFromPath(spark, source, fileUrl, srcSchema, dataSrcColumns);
                    dataframe = convertSrcDataframeToDstDataframe(dataframe, srcSchema,dstTableSchema);

                    if (!source.where.isEmpty()) {
                        dataframe = dataframe.filter(source.where);
                    }

                    dataframe = repartitionDataframeByBucketId(spark, dataframe,
                            partitionInfo, partitionKeyIndex,
                            partitionKeySchema, partitionRangeKeys,
                            keyColumnNames, valueColumnNames,
                            dstTableSchema, tableMeta);

                    processRollupTree(rootNode, dataframe, tableMeta, baseIndex);
                    /*
                    Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
                    nodeQueue.offer(rootNode);
                    int currentLevel = 0;
                    // level travel the tree
                    Map<Integer, Dataset<Row>> parentDataframeMap = new HashMap<>();
                    parentDataframeMap.put(baseIndex.indexId, dataframe);
                    Map<Integer, Dataset<Row>> childrenDataframeMap = new HashMap<>();
                    String pathPrefix = jobConf.output_path + "/" + jobLabel() + "/" + System.currentTimeMillis();
                    while (!nodeQueue.isEmpty()) {
                        RollupTreeNode curNode = nodeQueue.poll();
                        if (curNode.children != null) {
                            for (RollupTreeNode child : curNode.children) {
                                nodeQueue.offer(child);
                            }
                        }
                        currentLevel = processRollupTreeNode(curNode, currentLevel, tableMeta, pathPrefix, parentDataframeMap, childrenDataframeMap);
                    }
                    */
                }
            }
        }
        spark.stop();
    }
}
