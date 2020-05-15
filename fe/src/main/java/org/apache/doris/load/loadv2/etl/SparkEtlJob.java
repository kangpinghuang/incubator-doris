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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.doris.load.loadv2.dpp.SparkDpp;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlColumn;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlIndex;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlTable;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.functions;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SparkEtlJob is responsible for global dict building, data partition, data sort and data aggregation.
 * 1. init job config
 * 2. check if job has bitmap_dict function columns
 * 3. build global dict if step 2 is true
 * 4. dpp (data partition, data sort and data aggregation)
 */
public class SparkEtlJob {
    private static final String BITMAP_DICT_FUNC = "bitmap_dict";
    private static final String TO_BITMAP_FUNC = "to_bitmap";

    private String jobConfigFilePath;
    private EtlJobConfig etlJobConfig;
    private Map<Long, Set<String>> tableToBitmapDictColumns;
    private SparkSession spark;

    private SparkEtlJob(String jobConfigFilePath) {
        this.jobConfigFilePath = jobConfigFilePath;
        this.etlJobConfig = null;
        this.tableToBitmapDictColumns = Maps.newHashMap();
    }

    private void initSparkEnvironment() {
        spark = SparkSession.builder().enableHiveSupport().getOrCreate();
    }

    private void initConfig() {
        System.err.println("****** job config file path: " + jobConfigFilePath);
        Dataset<String> ds = spark.read().textFile(jobConfigFilePath);
        String jsonConfig = ds.first();
        System.err.println("****** rdd read json config: " + jsonConfig);
        etlJobConfig = EtlJobConfig.configFromJson(jsonConfig);
        System.err.println("****** etl job config: " + etlJobConfig.toString());
    }

    /*
     * 1. check bitmap column
     * 2. fill tableToBitmapDictColumns
     * 3. remove bitmap_dict and to_bitmap mapping from columnMappings
     */
    private void checkConfig() throws Exception {
        Map<Long, EtlTable> tables = etlJobConfig.tables;
        for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
            Set<String> bitmapDictColumns = Sets.newHashSet();
            for (EtlFileGroup fileGroup : entry.getValue().fileGroups) {
                Map<String, EtlColumnMapping> newColumnMappings = Maps.newHashMap();
                for (Map.Entry<String, EtlColumnMapping> mappingEntry : fileGroup.columnMappings.entrySet()) {
                    String columnName = mappingEntry.getKey();
                    String exprStr = mappingEntry.getValue().toDescription();
                    String funcName = functions.expr(exprStr).expr().prettyName();
                    if (funcName.equalsIgnoreCase(BITMAP_DICT_FUNC)) {
                        bitmapDictColumns.add(columnName);
                    } else if (!funcName.equalsIgnoreCase(TO_BITMAP_FUNC)) {
                        newColumnMappings.put(mappingEntry.getKey(), mappingEntry.getValue());
                    }
                }
                // reset new columnMappings
                // System.err.println("****** new column mappings: " + newColumnMappings);
                fileGroup.columnMappings = newColumnMappings;
            }
            if (!bitmapDictColumns.isEmpty()) {
                tableToBitmapDictColumns.put(entry.getKey(), bitmapDictColumns);
            }
        }
        System.err.println("****** tableToBitmapDictColumns: " + tableToBitmapDictColumns);

        // spark etl must have only one table with bitmap type column to process.
        if (tableToBitmapDictColumns.size() > 1) {
            throw new Exception("spark etl job must have only one table with bitmap type column to process");
        }
    }

    private void processDpp() throws Exception {
        SparkDpp sparkDpp = new SparkDpp(spark, etlJobConfig);
        sparkDpp.init();
        sparkDpp.doDpp();
    }

    private void buildGlobalDictAndEncodeSourceTable(EtlTable table, long tableId) {
        List<String> distinctColumnList = Lists.newArrayList(tableToBitmapDictColumns.get(tableId));
        List<String> dorisOlapTableColumnList = Lists.newArrayList();
        List<String> mapSideJoinColumns = Lists.newArrayList();
        List<EtlColumn> baseSchema = null;
        for (EtlIndex etlIndex : table.indexes) {
            if (etlIndex.isBaseIndex) {
                baseSchema = etlIndex.columns;
            }
        }
        for (EtlColumn column : baseSchema) {
            dorisOlapTableColumnList.add(column.columnName);
        }

        EtlFileGroup fileGroup = table.fileGroups.get(0);
        String sourceHiveDBTableName = fileGroup.hiveTableName;
        String dorisHiveDB = sourceHiveDBTableName.split("\\.")[0];
        String sourceHiveFilter = fileGroup.where;

        String taskId = etlJobConfig.outputPath.substring(etlJobConfig.outputPath.lastIndexOf("/") + 1);
        String globalDictTableName = String.format(EtlJobConfig.GLOBAL_DICT_TABLE_NAME, tableId);
        String distinctKeyTableName = String.format(EtlJobConfig.DISTINCT_KEY_TABLE_NAME, tableId, taskId);
        String dorisIntermediateHiveTable = String.format(EtlJobConfig.DORIS_INTERMEDIATE_HIVE_TABLE_NAME,
                                                          tableId, taskId);

        System.err.println("****** distinctColumnList: " + distinctColumnList);
        System.err.println("dorisOlapTableColumnList: " + dorisOlapTableColumnList);
        System.err.println("mapSideJoinColumns: " + mapSideJoinColumns);
        System.err.println("sourceHiveDBTableName: " + sourceHiveDBTableName);
        System.err.println("sourceHiveFilter: " + sourceHiveFilter);
        System.err.println("dorisHiveDB: " + dorisHiveDB);
        System.err.println("distinctKeyTableName: " + distinctKeyTableName);
        System.err.println("globalDictTableName: " + globalDictTableName);
        System.err.println("****** dorisIntermediateHiveTable: " + dorisIntermediateHiveTable);

        try {
            BuildGlobalDict buildGlobalDict = new BuildGlobalDict(distinctColumnList, dorisOlapTableColumnList,
                    mapSideJoinColumns, sourceHiveDBTableName,
                    sourceHiveFilter, dorisHiveDB, distinctKeyTableName,
                    globalDictTableName, dorisIntermediateHiveTable, spark);
            buildGlobalDict.createHiveIntermediateTable();
            buildGlobalDict.extractDistinctColumn();
            buildGlobalDict.buildGlobalDict();
            buildGlobalDict.encodeDorisIntermediateHiveTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processData() throws Exception {
        // build global dict if has bitmap dict columns
        if (!tableToBitmapDictColumns.isEmpty()) {
            // only one table
            long tableId = -1;
            EtlTable table = null;
            for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
                tableId = entry.getKey();
                table = entry.getValue();
                break;
            }
            // build global dict and encode source hive table
            buildGlobalDictAndEncodeSourceTable(table, tableId);
        }

        // data partition sort and aggregation
        processDpp();
    }

    private void run() throws Exception {
        initSparkEnvironment();
        initConfig();
        checkConfig();
        processData();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("missing job config file path arg");
            System.exit(-1);
        }

        try {
            new SparkEtlJob(args[0]).run();
        } catch (Exception e) {
            System.err.println("spark etl job run fail");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static class  BuildGlobalDict {

        protected static final Logger LOG = LoggerFactory.getLogger(BuildGlobalDict.class);

        // name of the column in doris table which need to build global dict
        // currently doris's table column name need to be consistent with the field name in the hive table
        // all column is lowercase
        // TODO(wb): add user customize map from source hive tabe column to doris column
        private List<String> distinctColumnList;
        // target doris table columns in current spark load job
        private List<String> dorisOlapTableColumnList;

        // distinct columns which need to use map join to solve data skew in encodeDorisIntermediateHiveTable()
        // we needn't to specify it until data skew happends
        private List<String> mapSideJoinColumns;

        // hive table datasource,format is db.table
        private String sourceHiveDBTableName;
        // user-specified filter when query sourceHiveDBTable
        private String sourceHiveFilter;
        // intermediate hive table to store the distinct value of distinct column
        private String distinctKeyTableName;
        // current doris table's global dict hive table
        private String globalDictTableName;

        // used for next step to read
        private String dorisIntermediateHiveTable;
        private SparkSession spark;

        // key=doris column name,value=column type
        private Map<String, String> dorisColumnNameTypeMap = new HashMap<>();

        public BuildGlobalDict(List<String> distinctColumnList,
                               List<String> dorisOlapTableColumnList,
                               List<String> mapSideJoinColumns,
                               String sourceHiveDBTableName,
                               String sourceHiveFilter,
                               String dorisHiveDB,
                               String distinctKeyTableName,
                               String globalDictTableName,
                               String dorisIntermediateHiveTable,
                               SparkSession spark) {
            this.distinctColumnList = distinctColumnList;
            this.dorisOlapTableColumnList = dorisOlapTableColumnList;
            this.mapSideJoinColumns = mapSideJoinColumns;
            this.sourceHiveDBTableName = sourceHiveDBTableName;
            this.sourceHiveFilter = sourceHiveFilter;
            this.distinctKeyTableName = distinctKeyTableName;
            this.globalDictTableName = globalDictTableName;
            this.dorisIntermediateHiveTable = dorisIntermediateHiveTable;
            this.spark = spark;

            LOG.info("global_key_word:" + sourceHiveDBTableName + "," + dorisIntermediateHiveTable + "," + distinctKeyTableName);

            spark.sql("use " + dorisHiveDB);
        }

        public void createHiveIntermediateTable() throws AnalysisException {
            LOG.info("createHiveIntermediateTable");
            Map<String, String> sourceHiveTableColumn = spark.catalog()
                    .listColumns(sourceHiveDBTableName)
                    .collectAsList()
                    .stream().collect(Collectors.toMap(Column::name, Column::dataType));

            Map<String, String> sourceHiveTableColumnInLowercase = new HashMap<>();
            for (Map.Entry<String, String> entry : sourceHiveTableColumn.entrySet()) {
                sourceHiveTableColumnInLowercase.put(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
            }

            // check and get doris column type in hive
            dorisOlapTableColumnList.stream().forEach(columnName -> {
                String columnType = sourceHiveTableColumnInLowercase.get(columnName);
                if (StringUtils.isEmpty(columnType)) {
                    throw new RuntimeException(String.format("doris column %s not in source hive table", columnName));
                }
                dorisColumnNameTypeMap.put(columnName, columnType);
            });

            // TODO(wb): drop hive table to prevent schema change
            // create IntermediateHiveTable
            spark.sql(getCreateIntermediateHiveTableSql());

            // insert data to IntermediateHiveTable
            spark.sql(getInsertIntermediateHiveTableSql());
        }

        public void extractDistinctColumn() {
            LOG.info("extractDistinctColumn");
            // create distinct tables
            // TODO(wb): maybe keep this table in memory ?
            spark.sql(getCreateDistinctKeyTableSql());

            // extract distinct column
            for (String column : distinctColumnList) {
                spark.sql(getInsertDistinctKeyTableSql(column, sourceHiveDBTableName));
            }
        }

        // TODO(wb): make build progress concurrently between columns
        // TODO(wb): support 1000 million rows newly added distinct values
        //          spark row_number function support max input about 100 million(more data would cause memoryOverHead,both in-heap and off-heap)
        //          Now I haven't seen such data scale scenario yet.But keep finding better solution is nessassary
        //          such as split data to multiple Dataset and use row_number function to deal separately
        public void buildGlobalDict() {
            LOG.info("buildGlobalDict");
            // create global dict hive table
            spark.sql(getCreateGlobalDictHiveTableSql());

            for (String distinctColumnName : distinctColumnList) {
                // get global dict max value
                List<Row> maxGlobalDictValueRow = spark.sql(getMaxGlobalDictValueSql(distinctColumnName)).collectAsList();
                if (maxGlobalDictValueRow.size() == 0) {
                    throw new RuntimeException(String.format("get max dict value failed: %s", distinctColumnName));
                }

                long maxDictValue = 0;
                long minDictValue = 0;
                Row row = maxGlobalDictValueRow.get(0);
                if (row != null && row.get(0) != null) {
                    maxDictValue = (long)row.get(0);
                    minDictValue = (long)row.get(1);
                }
                LOG.info(" column {} 's max value in dict is {} , min value is {}", distinctColumnName, maxDictValue, minDictValue);
                // maybe never happened, but we need detect it
                if (minDictValue < 0) {
                    throw new RuntimeException(String.format(" column  {} 's cardinality has exceed bigint's max value"));
                }

                // build global dict
                spark.sql(getCreateBuildGlobalDictSql(maxDictValue, distinctColumnName));
            }
        }

        // encode dorisIntermediateHiveTable's distinct column
        public void encodeDorisIntermediateHiveTable() {
            LOG.info("encodeDorisIntermediateHiveTable");
            for (String distinctColumn : distinctColumnList) {
                spark.sql(getEncodeDorisIntermediateHiveTableSql(distinctColumn));
            }
        }

        private String getCreateIntermediateHiveTableSql() {
            StringBuilder sql = new StringBuilder();
            sql.append("create table if not exists " + dorisIntermediateHiveTable + " ( ");

            dorisOlapTableColumnList.stream().forEach(columnName -> {
                sql.append(columnName).append(" ");
                if (distinctColumnList.contains(columnName)) {
                    sql.append(" string ,");
                } else {
                    sql.append(dorisColumnNameTypeMap.get(columnName)).append(" ,");
                }
            });
            return sql.deleteCharAt(sql.length() - 1).append(" )").append(" stored as sequencefile ").toString();
        }

        private String getInsertIntermediateHiveTableSql() {
            StringBuilder sql = new StringBuilder();
            sql.append("insert overwrite table ").append(dorisIntermediateHiveTable).append(" select ");
            dorisOlapTableColumnList.stream().forEach(columnName -> {
                sql.append(columnName).append(" ,");
            });
            sql.deleteCharAt(sql.length() - 1)
                    .append(" from ").append(sourceHiveDBTableName);
            if (!StringUtils.isEmpty(sourceHiveFilter)) {
                sql.append(" where ").append(sourceHiveFilter);
            }
            return sql.toString();
        }

        private String getCreateDistinctKeyTableSql() {
            return "create table if not exists " + distinctKeyTableName + "(dict_key string) partitioned by (dict_column string) stored as sequencefile ";
        }

        private String getInsertDistinctKeyTableSql(String distinctColumnName, String sourceHiveTable) {
            StringBuilder sql = new StringBuilder();
            sql.append("insert overwrite table ").append(distinctKeyTableName)
                    .append(" partition(dict_column='").append(distinctColumnName).append("')")
                    .append(" select ").append(distinctColumnName)
                    .append(" from ").append(sourceHiveTable)
                    .append(" group by ").append(distinctColumnName);
            return sql.toString();
        }

        private String getCreateGlobalDictHiveTableSql() {
            return "create table if not exists " + globalDictTableName
                    + "(dict_key string, dict_value bigint) partitioned by(dict_column string) stored as sequencefile ";
        }

        private String getMaxGlobalDictValueSql(String distinctColumnName) {
            return "select max(dict_value) as max_value,min(dict_value) as min_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "'";
        }

        private String getCreateBuildGlobalDictSql(long maxGlobalDictValue, String distinctColumnName) {
            return "insert overwrite table " + globalDictTableName + " partition(dict_column='" + distinctColumnName + "') "
                    + " select dict_key,dict_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "' "
                    + " union all select t1.dict_key as dict_key,(row_number() over(order by t1.dict_key)) + (" + maxGlobalDictValue + ") as dict_value from "
                    + "(select dict_key from " + distinctKeyTableName + " where dict_column='" + distinctColumnName + "' and dict_key is not null)t1 left join " +
                    " (select dict_key,dict_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "' )t2 " +
                    "on t1.dict_key = t2.dict_key where t2.dict_value is null";
        }

        private String getEncodeDorisIntermediateHiveTableSql(String distinctColumnName) {
            StringBuilder sql = new StringBuilder();
            sql.append("insert overwrite table ").append(dorisIntermediateHiveTable).append(" select ");
            // using map join to solve distinct column data skew
            // here is a spark sql hint
            if (mapSideJoinColumns.size() != 0 && mapSideJoinColumns.contains(distinctColumnName)) {
                sql.append(" /*+ BROADCAST (t) */ ");
            }
            dorisOlapTableColumnList.forEach(columnName -> {
                if (distinctColumnName.equals(columnName)) {
                    sql.append("t.dict_value").append(" ,");
                } else {
                    sql.append(dorisIntermediateHiveTable).append(".").append(columnName).append(" ,");
                }
            });
            sql.deleteCharAt(sql.length() - 1)
                    .append(" from ")
                    .append(dorisIntermediateHiveTable)
                    .append(" LEFT OUTER JOIN ( select dict_key,dict_value from ").append(globalDictTableName)
                    .append(" where dict_column='").append(distinctColumnName).append("' ) t on ")
                    .append(dorisIntermediateHiveTable).append(".").append(distinctColumnName)
                    .append(" = t.dict_key ");
            return sql.toString();
        }


    }

}