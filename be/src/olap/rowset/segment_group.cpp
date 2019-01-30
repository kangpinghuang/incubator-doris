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

#include "olap/rowset/segment_group.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>
#include <sstream>

#include "olap/rowset/column_data.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"
#include "olap/wrapper_field.h"
#include "olap/schema.h"

using std::ifstream;
using std::string;
using std::vector;

namespace doris {

#define SEGMENT_GROUP_PARAM_VALIDATE() \
    do { \
        if (!_index_loaded) { \
            OLAP_LOG_WARNING("fail to find, index is not loaded. [segment_group_id=%d]", \
                    _segment_group_id); \
            return OLAP_ERR_NOT_INITED; \
        } \
    } while (0);

#define POS_PARAM_VALIDATE(pos) \
    do { \
        if (NULL == pos) { \
            OLAP_LOG_WARNING("fail to find, NULL position parameter."); \
            return OLAP_ERR_INPUT_PARAMETER_ERROR; \
        } \
    } while (0);

#define SLICE_PARAM_VALIDATE(slice) \
    do { \
        if (NULL == slice) { \
            OLAP_LOG_WARNING("fail to find, NULL slice parameter."); \
            return OLAP_ERR_INPUT_PARAMETER_ERROR; \
        } \
    } while (0);

SegmentGroup::SegmentGroup(int64_t tablet_id, int64_t rowset_id, const TabletSchema* schema,
            const std::string& rowset_path_prefix, Version version, VersionHash version_hash,
            bool delete_flag, int32_t segment_group_id, int32_t num_segments)
      : _tablet_id(tablet_id),
        _rowset_id(rowset_id),
        _schema(schema),
        _rowset_path_prefix(rowset_path_prefix),
        _version(version),
        _version_hash(version_hash),
        _delete_flag(delete_flag),
        _segment_group_id(segment_group_id),
        _num_segments(num_segments) {
    _index_loaded = false;
    _ref_count = 0;
    _is_pending = false;
    _partition_id = 0;
    _txn_id = 0;
    _short_key_length = 0;
    _new_short_key_length = 0;
    _short_key_buf = nullptr;
    _file_created = false;
    _new_segment_created = false;
    _empty = false;

    for (size_t i = 0; i < _schema->num_short_key_columns(); ++i) {
        const TabletColumn& column = _schema->column(i);
        _short_key_columns.push_back(column);
        _short_key_length += column.index_length() + 1;// 1 for null byte
        if (column.type() == OLAP_FIELD_TYPE_CHAR ||
            column.type() == OLAP_FIELD_TYPE_VARCHAR) {
            _new_short_key_length += sizeof(Slice) + 1;
        } else {
            _new_short_key_length += column.index_length() + 1;
        }
    }
}

SegmentGroup::SegmentGroup(int64_t tablet_id, int64_t rowset_id, const TabletSchema* schema,
        const std::string& rowset_path_prefix, bool delete_flag,
        int32_t segment_group_id, int32_t num_segments, bool is_pending,
        TPartitionId partition_id, TTransactionId transaction_id) : _tablet_id(tablet_id),
        _rowset_id(rowset_id),
        _schema(schema),
        _rowset_path_prefix(rowset_path_prefix),
        _delete_flag(delete_flag),
        _segment_group_id(segment_group_id), _num_segments(num_segments),
        _is_pending(is_pending), _partition_id(partition_id),
        _txn_id(transaction_id) {
    _version = {-1, -1};
    _version_hash = 0;
    _load_id.set_hi(0);
    _load_id.set_lo(0);
    _index_loaded = false;
    _ref_count = 0;
    _short_key_length = 0;
    _new_short_key_length = 0;
    _short_key_buf = NULL;
    _file_created = false;
    _new_segment_created = false;
    _empty = false;

    for (size_t i = 0; i < _schema->num_key_columns(); ++i) {
        const TabletColumn& column = _schema->column(i);
        _short_key_columns.push_back(column);
        _short_key_length += column.index_length() + 1;// 1 for null byte
        if (column.type() == OLAP_FIELD_TYPE_CHAR
             || column.type() == OLAP_FIELD_TYPE_VARCHAR) {
            _new_short_key_length += sizeof(Slice) + 1;
        } else {
            _new_short_key_length += column.index_length() + 1;
        }
    }
}

SegmentGroup::~SegmentGroup() {
    delete [] _short_key_buf;
    _current_file_handler.close();

    for (size_t i = 0; i < _column_statistics.size(); ++i) {
        SAFE_DELETE(_column_statistics[i].first);
        SAFE_DELETE(_column_statistics[i].second);
    }
    _seg_pb_map.clear();
}

std::string SegmentGroup::_construct_pending_file_name(int32_t segment_id, const std::string& suffix) const {
    std::string pending_dir_path = _rowset_path_prefix + PENDING_DELTA_PREFIX;
    std::stringstream file_path;
    file_path << pending_dir_path << "/"
                          << std::to_string(_rowset_id) << "_" +  _txn_id
                          << _segment_group_id << "_" << segment_id << suffix;
    return file_path.str();
}

std::string SegmentGroup::_construct_file_name(int32_t segment_id, const string& suffix) const {
    std::string file_name = std::to_string(_rowset_id) + "_" + std::to_string(segment_id) + suffix;
    return file_name;
}

std::string SegmentGroup::construct_index_file_path(int32_t segment_id) const {
    std::stringstream file_path;
    file_path << _rowset_path_prefix << "/" << _tablet_id;
    if (_is_pending) {
        file_path << "/" << _construct_pending_file_name(segment_id, ".idx");
    } else {
        file_path << "/" << _construct_file_name(segment_id, ".idx");
    }
    return file_path.str();
}

std::string SegmentGroup::construct_data_file_path(int32_t segment_id) const {
    std::stringstream file_path;
    file_path << _rowset_path_prefix << "/" << _tablet_id;
    if (_is_pending) {
        file_path << "/" << _construct_pending_file_name(segment_id, ".dat");
    } else {
       file_path << "/" << _construct_file_name(segment_id, ".dat");
    }
    return file_path.str();
}

void SegmentGroup::acquire() {
    atomic_inc(&_ref_count);
}

int64_t SegmentGroup::ref_count() {
    return _ref_count;
}

void SegmentGroup::release() {
    atomic_dec(&_ref_count);
}

bool SegmentGroup::is_in_use() {
    return _ref_count > 0;
}

// you can not use SegmentGroup after delete_all_files(), or else unknown behavior occurs.
bool SegmentGroup::delete_all_files() {
    bool success = true;
    if (!_file_created) { return success; }
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        // get full path for one segment
        string index_path = construct_index_file_path(seg_id);
        string data_path = construct_data_file_path(seg_id);

        if (remove(index_path.c_str()) != 0) {
            char errmsg[64];
            LOG(WARNING) << "fail to delete index file. [err='" << strerror_r(errno, errmsg, 64)
                         << "' path='" << index_path << "']";
            success = false;
        }

        if (remove(data_path.c_str()) != 0) {
            char errmsg[64];
            LOG(WARNING) << "fail to delete data file. [err='" << strerror_r(errno, errmsg, 64)
                         << "' path='" << data_path << "']";
            success = false;
        }
    }
    return success;
}


OLAPStatus SegmentGroup::add_column_statistics_for_linked_schema_change(
        const std::vector<std::pair<WrapperField*, WrapperField*>>& column_statistic_fields) {
    //When add rollup tablet, the base tablet index maybe empty
    if (column_statistic_fields.size() == 0) {
        return OLAP_SUCCESS;
    }

    //Should use _num_key_columns, not column_statistic_fields.size()
    //as rollup tablet num_key_columns will less than base tablet column_statistic_fields.size().
    //For LinkedSchemaChange, the rollup tablet keys order is the same as base tablet
    for (size_t i = 0; i < _schema->num_key_columns(); ++i) {
        const TabletColumn& column = _schema->column(i);
        WrapperField* first = WrapperField::create(column);
        DCHECK(first != NULL) << "failed to allocate memory for field: " << i;
        first->copy(column_statistic_fields[i].first);

        WrapperField* second = WrapperField::create(column);
        DCHECK(second != NULL) << "failed to allocate memory for field: " << i;
        second->copy(column_statistic_fields[i].second);

        _column_statistics.push_back(std::make_pair(first, second));
    }
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::add_column_statistics(
        const std::vector<std::pair<WrapperField*, WrapperField*>>& column_statistic_fields) {
    DCHECK(column_statistic_fields.size() == _schema->num_key_columns());
    for (size_t i = 0; i < column_statistic_fields.size(); ++i) {
        const TabletColumn& column = _schema->column(i);
        WrapperField* first = WrapperField::create(column);
        DCHECK(first != NULL) << "failed to allocate memory for field: " << i;
        first->copy(column_statistic_fields[i].first);

        WrapperField* second = WrapperField::create(column);
        DCHECK(second != NULL) << "failed to allocate memory for field: " << i;
        second->copy(column_statistic_fields[i].second);

        _column_statistics.push_back(std::make_pair(first, second));
    }
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::add_column_statistics(
        std::vector<std::pair<std::string, std::string> > &column_statistic_strings,
        std::vector<bool> &null_vec) {
    DCHECK(column_statistic_strings.size() == _schema->num_key_columns());
    for (size_t i = 0; i < column_statistic_strings.size(); ++i) {
        const TabletColumn& column = _schema->column(i);
        WrapperField* first = WrapperField::create(column);
        DCHECK(first != NULL) << "failed to allocate memory for field: " << i ;
        RETURN_NOT_OK(first->from_string(column_statistic_strings[i].first));
        if (null_vec[i]) {
            //[min, max] -> [NULL, max]
            first->set_null();
        }
        WrapperField* second = WrapperField::create(column);
        DCHECK(first != NULL) << "failed to allocate memory for field: " << i ;
        RETURN_NOT_OK(second->from_string(column_statistic_strings[i].second));
        _column_statistics.push_back(std::make_pair(first, second));
    }
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::load() {
    if (_empty) {
        return OLAP_SUCCESS;
    }
    OLAPStatus res = OLAP_ERR_INDEX_LOAD_ERROR;
    boost::lock_guard<boost::mutex> guard(_index_load_lock);

    if (_index_loaded) {
        return OLAP_SUCCESS;
    }

    if (_num_segments == 0) {
        OLAP_LOG_WARNING("fail to load index, segments number is 0.");
        return res;
    }

    if (_index.init(_short_key_length, _new_short_key_length,
                    _schema->num_short_key_columns(), &_short_key_columns) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to create MemIndex. [num_segment=%d]", _num_segments);
        return res;
    }

    // for each segment
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        string seg_path = construct_data_file_path(seg_id);
        if (OLAP_SUCCESS != (res = load_pb(seg_path.c_str(), seg_id))) {
            LOG(WARNING) << "failed to load pb structures. [seg_path='" << seg_path << "']";
            
            return res;
        }
        
        // get full path for one segment
        std::string path = construct_index_file_path(seg_id);
        if ((res = _index.load_segment(path.c_str(), &_current_num_rows_per_row_block))
                != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load segment. [path='" << path << "']";
            
            return res;
        }
    }

    _delete_flag = _index.delete_flag();
    _index_loaded = true;
    _file_created = true;

    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::load_pb(const char* file, uint32_t seg_id) {
    OLAPStatus res = OLAP_SUCCESS;

    FileHeader<ColumnDataHeaderMessage> seg_file_header;
    FileHandler seg_file_handler;
    res = seg_file_handler.open(file, O_RDONLY);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("failed to open segment file. [err=%d, file=%s]", res, file);
        return res;
    }

    res = seg_file_header.unserialize(&seg_file_handler);
    if (OLAP_SUCCESS != res) {
        seg_file_handler.close();
        OLAP_LOG_WARNING("fail to unserialize header. [err=%d, path='%s']", res, file);
        return res;
    }

    _seg_pb_map[seg_id] = seg_file_header;
    seg_file_handler.close();
    return OLAP_SUCCESS;
}

bool SegmentGroup::index_loaded() {
    return _index_loaded;
}

OLAPStatus SegmentGroup::validate() {
    if (_empty) {
        return OLAP_SUCCESS;
    }

    OLAPStatus res = OLAP_SUCCESS;
    for (uint32_t seg_id = 0; seg_id < _num_segments; ++seg_id) {
        FileHeader<OLAPIndexHeaderMessage, OLAPIndexFixedHeader> index_file_header;
        FileHeader<OLAPDataHeaderMessage> data_file_header;

        // get full path for one segment
        string index_path = construct_index_file_path(seg_id);
        string data_path = construct_data_file_path(seg_id);

        // 检查index文件头
        if ((res = index_file_header.validate(index_path)) != OLAP_SUCCESS) {
            LOG(WARNING) << "validate index file error. [file='" << index_path << "']"; 
            return res;
        }

        // 检查data文件头
        if ((res = data_file_header.validate(data_path)) != OLAP_SUCCESS) {
            LOG(WARNING) << "validate data file error. [file='" << data_path << "']";  
            return res;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::find_row_block(const RowCursor& key,
                                 RowCursor* helper_cursor,
                                 bool find_last,
                                 RowBlockPosition* pos) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(pos);

    // 将这部分逻辑从memindex移出来，这样可以复用find。
    OLAPIndexOffset offset = _index.find(key, helper_cursor, find_last);
    if (offset.offset > 0) {
        offset.offset = offset.offset - 1;
    } else {
        offset.offset = 0;
    }

    if (find_last) {
        OLAPIndexOffset next_offset = _index.next(offset);
        if (!(next_offset == _index.end())) {
            offset = next_offset;
        }
    }

    return _index.get_row_block_position(offset, pos);
}

OLAPStatus SegmentGroup::find_short_key(const RowCursor& key,
                                 RowCursor* helper_cursor,
                                 bool find_last,
                                 RowBlockPosition* pos) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(pos);

    // 由于find会从前一个segment找起，如果前一个segment中恰好没有该key，
    // 就用前移后移来移动segment的位置.
    OLAPIndexOffset offset = _index.find(key, helper_cursor, find_last);
    if (offset.offset > 0) {
        offset.offset = offset.offset - 1;

        OLAPIndexOffset next_offset = _index.next(offset);
        if (!(next_offset == _index.end())) {
            offset = next_offset;
        }
    }

    VLOG(3) << "seg=" << offset.segment << ", offset=" << offset.offset;
    return _index.get_row_block_position(offset, pos);
}

OLAPStatus SegmentGroup::get_row_block_entry(const RowBlockPosition& pos, EntrySlice* entry) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    SLICE_PARAM_VALIDATE(entry);
    
    return _index.get_entry(_index.get_offset(pos), entry);
}

OLAPStatus SegmentGroup::find_first_row_block(RowBlockPosition* position) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);
    
    return _index.get_row_block_position(_index.find_first(), position);
}

OLAPStatus SegmentGroup::find_last_row_block(RowBlockPosition* position) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);
    
    return _index.get_row_block_position(_index.find_last(), position);
}

OLAPStatus SegmentGroup::find_next_row_block(RowBlockPosition* pos, bool* eof) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(pos);
    POS_PARAM_VALIDATE(eof);

    OLAPIndexOffset current = _index.get_offset(*pos);
    *eof = false;

    OLAPIndexOffset next = _index.next(current);
    if (next == _index.end()) {
        *eof = true;
        return OLAP_ERR_INDEX_EOF;
    }

    return _index.get_row_block_position(next, pos);
}

OLAPStatus SegmentGroup::find_mid_point(const RowBlockPosition& low,
                                 const RowBlockPosition& high,
                                 RowBlockPosition* output,
                                 uint32_t* dis) const {
    *dis = compute_distance(low, high);
    if (*dis >= _index.count()) {
        return OLAP_ERR_INDEX_EOF;
    } else {
        *output = low;
        if (advance_row_block(*dis / 2, output) != OLAP_SUCCESS) {
            return OLAP_ERR_INDEX_EOF;
        }

        return OLAP_SUCCESS;
    }
}

OLAPStatus SegmentGroup::find_prev_point(
        const RowBlockPosition& current, RowBlockPosition* prev) const {
    OLAPIndexOffset current_offset = _index.get_offset(current);
    OLAPIndexOffset prev_offset = _index.prev(current_offset);

    return _index.get_row_block_position(prev_offset, prev);
}

OLAPStatus SegmentGroup::advance_row_block(int64_t num_row_blocks, RowBlockPosition* position) const {
    SEGMENT_GROUP_PARAM_VALIDATE();
    POS_PARAM_VALIDATE(position);

    OLAPIndexOffset off = _index.get_offset(*position);
    iterator_offset_t absolute_offset = _index.get_absolute_offset(off) + num_row_blocks;
    if (absolute_offset >= _index.count()) {
        return OLAP_ERR_INDEX_EOF;
    }

    return _index.get_row_block_position(_index.get_relative_offset(absolute_offset), position);
}

// PRECONDITION position1 < position2
uint32_t SegmentGroup::compute_distance(const RowBlockPosition& position1,
                                     const RowBlockPosition& position2) const {
    iterator_offset_t offset1 = _index.get_absolute_offset(_index.get_offset(position1));
    iterator_offset_t offset2 = _index.get_absolute_offset(_index.get_offset(position2));

    return offset2 > offset1 ? offset2 - offset1 : 0;
}

OLAPStatus SegmentGroup::add_segment() {
    // 打开文件
    ++_num_segments;

    OLAPIndexHeaderMessage* index_header = NULL;
    // 构造Proto格式的Header
    index_header = _file_header.mutable_message();
    index_header->set_start_version(_version.first);
    index_header->set_end_version(_version.second);
    index_header->set_cumulative_version_hash(_version_hash);
    index_header->set_segment(_num_segments - 1);
    index_header->set_num_rows_per_block(_schema->num_rows_per_row_block());
    index_header->set_delete_flag(_delete_flag);
    index_header->set_null_supported(true);

    // 分配一段存储short key的内存, 初始化index_row
    if (_short_key_buf == NULL) {
        _short_key_buf = new(std::nothrow) char[_short_key_length];
        if (_short_key_buf == NULL) {
            OLAP_LOG_WARNING("malloc short_key_buf error.");
            return OLAP_ERR_MALLOC_ERROR;
        }

        if (_current_index_row.init(*_schema) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init _current_index_row fail.");
            return OLAP_ERR_INIT_FAILED;
        }
    }

    // 初始化checksum
    _checksum = ADLER32_INIT;
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::add_row_block(const RowBlock& row_block, const uint32_t data_offset) {
    // get first row of the row_block to distill index item.
    row_block.get_row(0, &_current_index_row);
    return add_short_key(_current_index_row, data_offset);
}

OLAPStatus SegmentGroup::add_short_key(const RowCursor& short_key, const uint32_t data_offset) {
    OLAPStatus res = OLAP_SUCCESS;
    if (!_new_segment_created) {
        string file_path = construct_index_file_path(_num_segments - 1);
        res = _current_file_handler.open_with_mode(
                        file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
        if (res != OLAP_SUCCESS) {
            char errmsg[64];
            LOG(WARNING) << "can not create file. [file_path='" << file_path
                << "' err='" << strerror_r(errno, errmsg, 64) << "']";
            return res;
        }
        _file_created = true;
        _new_segment_created = true;

        // 准备FileHeader
        if ((res = _file_header.prepare(&_current_file_handler)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("write file header error. [err=%m]");
            return res;
        }

        // 跳过FileHeader
        if (_current_file_handler.seek(_file_header.size(), SEEK_SET) == -1) {
            OLAP_LOG_WARNING("lseek header file error. [err=%m]");
            res = OLAP_ERR_IO_ERROR;
            return res;
        }
    }

    // 将short key的内容写入_short_key_buf
    size_t offset = 0;

    //short_key.write_null_array(_short_key_buf);
    //offset += short_key.get_num_null_byte();
    for (size_t i = 0; i < _short_key_columns.size(); i++) {
        short_key.write_index_by_index(i, _short_key_buf + offset);
        offset += short_key.get_index_size(i) + 1;
    }

    // 写入Short Key对应的数据
    if ((res = _current_file_handler.write(_short_key_buf, _short_key_length)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("write short key failed. [err=%m]");
        
        return res;
    }

    // 写入对应的数据文件偏移量
    if ((res = _current_file_handler.write(&data_offset, sizeof(data_offset))) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("write data_offset failed. [err=%m]");
        return res;
    }

    _checksum = olap_adler32(_checksum, _short_key_buf, _short_key_length);
    _checksum = olap_adler32(_checksum,
                             reinterpret_cast<const char*>(&data_offset),
                             sizeof(data_offset));
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::finalize_segment(uint32_t data_segment_size, int64_t num_rows) {
    // 准备FileHeader
    OLAPStatus res = OLAP_SUCCESS;

    int file_length = _current_file_handler.tell();
    if (file_length == -1) {
        OLAP_LOG_WARNING("get file_length error. [err=%m]");
        
        return OLAP_ERR_IO_ERROR;
    }

    _file_header.set_file_length(file_length);
    _file_header.set_checksum(_checksum);
    _file_header.mutable_extra()->data_length = data_segment_size;
    _file_header.mutable_extra()->num_rows = num_rows;

    // 写入更新之后的FileHeader
    if ((res = _file_header.serialize(&_current_file_handler)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("write file header error. [err=%m]");
        
        return res;
    }

    VLOG(3) << "finalize_segment. file_name=" << _current_file_handler.file_name()
            << ", file_length=" << file_length;

    if ((res = _current_file_handler.close()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("close file error. [err=%m]");
        
        return res;
    }

    _new_segment_created = false;
    return OLAP_SUCCESS;
}

uint64_t SegmentGroup::num_index_entries() const {
    return _index.count();
}

size_t SegmentGroup::current_num_rows_per_row_block() const {
    return _current_num_rows_per_row_block;
}

const TabletSchema& SegmentGroup::get_tablet_schema() {
    return *_schema;
}

int SegmentGroup::get_num_key_columns() {
    return _schema->num_key_columns();
}

int SegmentGroup::get_num_short_key_columns() {
    return _schema->num_short_key_columns();
}

size_t SegmentGroup::get_num_rows_per_row_block() {
    return _schema->num_rows_per_row_block();
}

std::string SegmentGroup::get_rowset_path_prefix() {
    return _rowset_path_prefix;
}

int64_t SegmentGroup::get_tablet_id() {
    return _tablet_id;
}

OLAPStatus SegmentGroup::make_snapshot(std::vector<std::string>* success_links) {
    for (int segment_id = 0; segment_id < _num_segments; segment_id++) {
        std::string new_data_file_name = construct_data_file_path(segment_id);
        if (!check_dir_existed(new_data_file_name)) {
            std::string old_data_file_name = construct_old_data_file_path(segment_id);
            if (link(new_data_file_name.c_str(), old_data_file_name.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link. from=" << old_data_file_name << ", "
                    << "to=" << new_data_file_name << ", " << "errno=" << Errno::no();
                return OLAP_ERR_OS_ERROR;
            }
        }
        success_links->push_back(new_data_file_name);
        std::string new_index_file_name = construct_index_file_path(segment_id);
        if (!check_dir_existed(new_index_file_name)) {
            std::string old_index_file_name = construct_old_index_file_path(segment_id);
            if (link(new_index_file_name.c_str(), old_index_file_name.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link. from=" << old_index_file_name << ", "
                    << "to=" << new_index_file_name << ", " << "errno=" << Errno::no();
                return OLAP_ERR_OS_ERROR;
            }
        }
        success_links->push_back(new_index_file_name);
    }
    return OLAP_SUCCESS;
}

OLAPStatus SegmentGroup::remove_old_files(std::vector<std::string>* links_to_remove) {
    for (int segment_id = 0; segment_id < _num_segments; segment_id++) {
        std::string old_data_file_name = construct_old_data_file_path(segment_id);
        RETURN_NOT_OK(remove_dir(old_data_file_name));
        links_to_remove->push_back(old_data_file_name);
        std::string old_index_file_name = construct_old_index_file_path(segment_id);
        RETURN_NOT_OK(remove_dir(old_index_file_name));
        links_to_remove->push_back(old_index_file_name);
    }
    return OLAP_SUCCESS;
}

bool SegmentGroup::copy_segments_to_path(const std::string& dest_path) {
    if (dest_path.empty() || dest_path == _rowset_path_prefix) {
        return true;
    }
    for (int segment_id = 0; segment_id < _num_segments; segment_id++) {
        std::string data_file_name = _construct_file_name(segment_id, ".dat");
        std::string new_data_file_path = dest_path + "/" + data_file_name;
        if (!check_dir_existed(new_data_file_path)) {
            std::string origin_data_file_path = construct_data_file_path(segment_id);
            if (link(new_data_file_path.c_str(), origin_data_file_path.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link. from=" << origin_data_file_path << ", "
                    << "to=" << new_data_file_path << ", " << "errno=" << Errno::no();
                return false;
            }
        }
        std::string index_file_name = _construct_file_name(segment_id, ".idx");
        std::string new_index_file_path = dest_path + "/" + index_file_name;
        if (!check_dir_existed(new_index_file_path)) {
            std::string origin_idx_file_path = construct_data_file_path(segment_id);
            if (link(new_index_file_path.c_str(), origin_idx_file_path.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link. from=" << origin_idx_file_path << ", "
                    << "to=" << new_index_file_path << ", " << "errno=" << Errno::no();
                return false;
            }
        }
    }
    return true;
}

std::string SegmentGroup::construct_old_index_file_path(int32_t segment_id) const {
    if (_is_pending) {
        return _construct_old_pending_file_path(segment_id, ".idx");
    } else {
        return _construct_old_file_path(segment_id, ".idx");
    }
}
    
std::string SegmentGroup::construct_old_data_file_path(int32_t segment_id) const {
    if (_is_pending) {
        return _construct_old_pending_file_path(segment_id, ".dat");
    } else {
        return _construct_old_file_path(segment_id, ".dat");
    }
}

std::string SegmentGroup::_construct_old_pending_file_path(int32_t segment_id,
    const std::string& suffix) const {
    std::string dir_path = _rowset_path_prefix + PENDING_DELTA_PREFIX;
    std::stringstream file_path;
    file_path << dir_path << "/"
                          << _txn_id << "_"
                          << _segment_group_id << "_" << segment_id << suffix;
    return file_path.str();
}
    
std::string SegmentGroup::_construct_old_file_path(int32_t segment_id, const std::string& suffix) const {
    char file_path[OLAP_MAX_PATH_LEN];
    if (_segment_group_id == -1) {
        snprintf(file_path,
                 sizeof(file_path),
                 "%s_%ld_%ld_%ld_%d.%s",
                 _rowset_path_prefix.c_str(),
                 _version.first,
                 _version.second,
                 _version_hash,
                 segment_id,
                 suffix.c_str());
    } else {
        snprintf(file_path,
                 sizeof(file_path),
                 "%s_%ld_%ld_%ld_%d_%d.%s",
                 _rowset_path_prefix.c_str(),
                 _version.first,
                 _version.second,
                 _version_hash,
                 _segment_group_id, segment_id,
                 suffix.c_str());
    }

    return file_path;
}

} // namespace doris
