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

package org.apache.doris.catalog;

import org.apache.doris.analysis.ModifyEtlClusterClause;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public abstract class EtlCluster implements Writable {
    public enum EtlClusterType {
        SPARK("spark");

        private final String typeName;

        private EtlClusterType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public static EtlClusterType fromString(String clusterType) {
            for (EtlClusterType type : EtlClusterType.values()) {
                if (type.getTypeName().equalsIgnoreCase(clusterType)) {
                    return type;
                }
            }
            return null;
        }
    }

    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected EtlClusterType type;

    public EtlCluster(String name, EtlClusterType type) {
        this.name = name;
        this.type = type;
    }

    public static EtlCluster fromClause(ModifyEtlClusterClause clause) throws DdlException {
        EtlCluster etlCluster = null;
        EtlClusterType clusterType = clause.getClusterType();
        switch (clusterType) {
            case SPARK:
                etlCluster = new SparkEtlCluster(clause.getClusterName());
                break;
            default:
                throw new DdlException("Only support Spark cluster.");
        }

        etlCluster.setProperties(clause.getProperties());
        return etlCluster;
    }

    public String getName() {
        return name;
    }

    public EtlClusterType getType() {
        return type;
    }

    protected abstract void setProperties(Map<String, String> properties) throws DdlException;

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static EtlCluster readIn(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, EtlCluster.class);
    }
}

