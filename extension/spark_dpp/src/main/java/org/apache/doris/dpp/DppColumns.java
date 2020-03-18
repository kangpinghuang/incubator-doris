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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

class DppColumns implements Comparable<DppColumns>, Serializable {
    public List<Object> columns;
    public List<Class> schema;

    public DppColumns() {
        columns = null;
        schema = null;
    }

    public DppColumns(List<Object> keys, List<Class> schema){
        this.columns = keys;
        this.schema = schema;

    }

    public DppColumns(DppColumns key, List<Integer> indexes){
        columns = new ArrayList<Object>();
        schema = new ArrayList<Class>();
        for (int i = 0; i < indexes.size(); ++i) {
            columns.add(key.columns.get(indexes.get(i)));
            schema.add(key.schema.get(indexes.get(i)));
        }
    }

    @Override
    public int compareTo(DppColumns other) {
        assert (columns.size() == other.columns.size());
        assert (schema.size() == other.schema.size());

        int cmp = 0;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) instanceof Integer) {
                cmp = ((Integer)(this.columns.get(i))).compareTo((Integer)(other.columns.get(i)));
            } else if (columns.get(i) instanceof Long) {
                cmp = ((Long)(this.columns.get(i))).compareTo((Long)(other.columns.get(i)));
            }  else if (columns.get(i) instanceof  Boolean) {
                cmp = ((Long)(this.columns.get(i))).compareTo((Long)(other.columns.get(i)));
            } else if (columns.get(i) instanceof  Short) {
                cmp = ((Short)(this.columns.get(i))).compareTo((Short)(other.columns.get(i)));
            } else if (columns.get(i) instanceof  Float) {
                cmp = ((Short)(this.columns.get(i))).compareTo((Short)(other.columns.get(i)));
            }
            if (cmp != 0) {
                return cmp;
            }
        }
        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DppColumns dppColumns = (DppColumns) o;
        return Objects.equals(columns, dppColumns.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public String toString() {
        return "dppColumns{" +
                "keys=" + columns.toString() +
                '}';
    }
}

class DppColumnsComparator implements Comparator<DppColumns> {
    @Override
    public int compare(DppColumns left, DppColumns right) {
        return left.compareTo(right);
    }
}