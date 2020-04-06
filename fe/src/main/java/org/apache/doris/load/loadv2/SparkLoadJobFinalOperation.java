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

import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * for Spark load job final state
 */
public class SparkLoadJobFinalOperation extends LoadJobFinalOperation {
    private long etlStartTimestamp;

    public SparkLoadJobFinalOperation() {
        super();
    }

    public SparkLoadJobFinalOperation(long id, EtlStatus loadingStatus, int progress, long etlStartTimestamp,
                                      long loadStartTimestamp, long finishTimestamp, JobState jobState,
                                      FailMsg failMsg) {
        super(id, loadingStatus, progress, loadStartTimestamp, finishTimestamp, jobState, failMsg);
        this.etlStartTimestamp = etlStartTimestamp;
    }

    public long getEtlStartTimestamp() {
        return etlStartTimestamp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(etlStartTimestamp);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        etlStartTimestamp = in.readLong();
    }
}
