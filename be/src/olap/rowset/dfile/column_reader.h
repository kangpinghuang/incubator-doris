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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_DFILE_COLUMN_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_DFILE_COLUMN_READER_H

#include "runtime/vectorized_row_batch.h"

namespace doris {

namespace dfile {

class ColumnReader {
public:
    ColumnReader() { }

    bool init();

    // Seek to the first entry in the column.
    bool seek_to_first();

    // Seek to the given ordinal entry in the column.
    // Entry 0 is the first entry written to the column.
    // If provided seek point is past the end of the file,
    // then returns false.
    bool seek_to_ordinal(rowid_t ord_idx) override;

    // Fetch the next vector of values from the page into 'dst'.
    // The output vector must have space for up to n cells.
    //
    // return the size of entries.
    //
    // In the case that the values are themselves references
    // to other memory (eg Slices), the referred-to memory is
    // allocated in the dst column vector's arena.
    virtual size_t next_vector(const size_t n, ColumnVector *dst) = 0;

    size_t get_current_oridinal();

    // 每个批次读取的时候，会调用这个函数
    bool prepare_batch(size_t n);

    // 释放batch读取的相关资源
    bool finish_batch();
};

}  // namespace dfile

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_DFILE_COLUMN_READER_H
