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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.load.EtlJobType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class SparkLoadJob extends BulkLoadJob {
    private static final Logger LOG = LogManager.getLogger(SparkLoadJob.class);

    //for global dict
    private static final String BITMAP_DATA_PROPERTY = "bitmap_data";

    private EtlClusterDesc etlClusterDesc;

    private long etlStartTimestamp = -1;

    // hivedb.table for global dict
    private String bitmapDataHiveTable = "";

    // only for log replay
    public SparkLoadJob() {
        super();
        this.jobType = EtlJobType.SPARK;
    }

    SparkLoadJob(long dbId, String label, EtlClusterDesc etlClusterDesc, String originStmt)
            throws MetaNotFoundException {
        super(dbId, label, originStmt);
        // broker timeout？
        this.timeoutSecond = Config.broker_load_default_timeout_second;
        this.etlClusterDesc = etlClusterDesc;
        this.jobType = EtlJobType.SPARK;
    }

    public String getBitmapDataHiveTable() {
        return bitmapDataHiveTable;
    }

    @Override
    protected void setJobProperties(Map<String, String> properties) throws DdlException {
        super.setJobProperties(properties);

        // global dict
        if (properties != null) {
            if (properties.containsKey(BITMAP_DATA_PROPERTY)) {
                bitmapDataHiveTable = properties.get(BITMAP_DATA_PROPERTY);
            }
        }
    }

    @Override
    protected void unprotectedExecuteJob() throws LoadException {
        LoadTask task = new SparkLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(),
                                                 etlClusterDesc);
        task.init();
        idToTasks.put(task.getSignature(), task);
        Catalog.getCurrentCatalog().getLoadTaskScheduler().submit(task);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        etlClusterDesc.write(out);
        Text.writeString(out, bitmapDataHiveTable);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in, null);
        etlClusterDesc = EtlClusterDesc.read(in);
        bitmapDataHiveTable = Text.readString(in);
    }
}
