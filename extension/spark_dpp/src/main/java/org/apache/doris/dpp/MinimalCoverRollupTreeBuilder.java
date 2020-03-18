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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// Build RollupTree by using minimal cover strategy
public class MinimalCoverRollupTreeBuilder implements RollupTreeBuilder {
    public RollupTreeNode build(TableMeta tableMeta) {
        List<ColumnMeta> columns = tableMeta.columns;
        List<IndexMeta> indexes = tableMeta.indexes;
        List<String> baseKeyColumns = new ArrayList<>();
        List<String> baseValueColumns = new ArrayList<>();
        for (ColumnMeta columnMeta : columns) {
            if (columnMeta.isKey) {
                baseKeyColumns.add(columnMeta.columnName);
            } else {
                baseValueColumns.add(columnMeta.columnName);
            }

        }
        RollupTreeNode root = new RollupTreeNode();
        root.parent = null;
        root.keyColumnNames = baseKeyColumns;
        root.valueColumnNames = baseValueColumns;
        root.indexId = tableMeta.baseIndex;

        // post order traverse the tree
        List<IndexMeta> indexMetas = new ArrayList<>();
        for (IndexMeta indexMeta : indexes) {
            if (indexMeta.indexId == tableMeta.baseIndex) {
                root.indexMeta = indexMeta;
                continue;
            }
            indexMetas.add(indexMeta);
        }
        System.err.println("before sorted index metas:" + indexMetas);
        // sort the index metas to make sure the column number decrease
        Collections.sort(indexMetas, new IndexMetaColumnComparator().reversed());
        System.err.println("after sorted index metas:" + indexMetas);
        boolean[] flags = new boolean[indexMetas.size()];
        for (int i = 0; i < indexMetas.size(); ++i) {
            flags[i] = false;
        }
        for (int i = 0; i < indexMetas.size(); ++i) {
            List<String> keyColumns = new ArrayList<>();
            List<String> valueColumns = new ArrayList<>();
            for (IndexMeta.IndexColumnDescription columnDescription : indexMetas.get(i).columnDescriptions) {
                if (columnDescription.isKey) {
                    keyColumns.add(columnDescription.name);
                } else {
                    valueColumns.add(columnDescription.name);
                }
            }
            insertIndex(root, indexMetas.get(i), keyColumns, valueColumns, i, flags);
        }
        return root;
    }

    private void insertIndex(RollupTreeNode root, IndexMeta indexMeta, List<String> keyColumns, List<String> valueColumns, int id, boolean[] flags) {
        if (root.children != null) {
            for (RollupTreeNode child : root.children) {
                insertIndex(child, indexMeta, keyColumns, valueColumns, id, flags);
                if (flags[id]) {
                    return;
                }
            }
        }
        if (flags[id]) {
            return;
        }
        if (root.keyColumnNames.containsAll(keyColumns) && root.valueColumnNames.containsAll(valueColumns)) {
            if (root.children == null) {
                root.children = new ArrayList<>();
            }
            RollupTreeNode newChild = new RollupTreeNode();
            newChild.keyColumnNames = keyColumns;
            newChild.valueColumnNames = valueColumns;
            newChild.indexMeta = indexMeta;
            newChild.indexId = indexMeta.indexId;
            newChild.parent = root;
            newChild.level = root.level + 1;
            root.children.add(newChild);
            flags[id] = true;
            System.err.println("root index:" + root.indexId + " add new child:" + newChild.indexId);
        }
    }
}
