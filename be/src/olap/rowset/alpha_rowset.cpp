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

#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"

namespace doris {

AlphaRowset::AlphaRowset(const RowFields& tablet_schema,
        int num_key_fields, int num_short_key_fields,
        int num_rows_per_row_block, const std::string rowset_path,
        RowsetMetaSharedPtr rowset_meta) : _tablet_schema(tablet_schema),
        _num_key_fields(num_key_fields),
        _num_short_key_fields(num_short_key_fields),
        _num_rows_per_row_block(num_rows_per_row_block),
        _rowset_path(rowset_path),
        _rowset_meta(rowset_meta),
        _segment_group_size(0),
        _is_cumulative_rowset(false),
        _is_pending_rowset(false) {
    if (!_rowset_meta->has_version()) {
        _is_pending_rowset = true;
    }
    if (!_is_pending_rowset) {
        Version version = _rowset_meta->version();
        if (version.first == version.second) {
            _is_cumulative_rowset = false;
        } else {
            _is_cumulative_rowset = true;
        }
    }

}

OLAPStatus AlphaRowset::init() {
    _init_segment_groups();
    return OLAP_SUCCESS;
}

std::unique_ptr<RowsetReader> AlphaRowset::create_reader() {
    return std::unique_ptr<RowsetReader>(new AlphaRowsetReader(
            _num_key_fields, _num_short_key_fields, _num_rows_per_row_block,
            _rowset_path, _rowset_meta.get(), _segment_groups));
}

OLAPStatus AlphaRowset::copy(RowsetBuilder* dest_rowset_builder) {
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::remove() {
    // TODO(hkp) : add delete code
    // delete rowset from meta
    // delete segment groups 
    return OLAP_SUCCESS;
}

RowsetMetaSharedPtr AlphaRowset::rowset_meta() const {
    return _rowset_meta;
}

void AlphaRowset::set_version(Version version) {
    _rowset_meta->set_version(version);
    _is_pending_rowset = false;
}

bool AlphaRowset::create_hard_links(std::vector<std::string>* success_links) {
    for (auto segment_group : _segment_groups) {
        bool  ret = segment_group->create_hard_links(success_links);
        if (!ret) {
            LOG(WARNING) << "create hard links failed for segment group:"
                << segment_group->segment_group_id();
            return false;
        }
    }
    return true;
}

bool AlphaRowset::remove_old_files(std::vector<std::string>* removed_links) {
    for (auto segment_group : _segment_groups) {
        bool  ret = segment_group->remove_old_files(removed_links);
        if (!ret) {
            LOG(WARNING) << "remove old files failed for segment group:"
                << segment_group->segment_group_id();
            return false;
        }
    }
    return true;
}

int AlphaRowset::data_disk_size() const {
    return _rowset_meta->total_disk_size();
}

int AlphaRowset::index_disk_size() const {
    return _rowset_meta->index_disk_size();
}

bool AlphaRowset::empty() const {
    return _rowset_meta->empty();
}

bool AlphaRowset::zero_num_rows() const {
    return _rowset_meta->row_number() == 0;
}

size_t AlphaRowset::num_rows() const {
    return _rowset_meta->row_number();
}

OLAPStatus AlphaRowset::_init_segment_groups() {
    std::vector<SegmentGroupPB> segment_group_metas;
    AlphaRowsetMeta* _alpha_rowset_meta = (AlphaRowsetMeta*)_rowset_meta.get();
    _alpha_rowset_meta->get_segment_groups(&segment_group_metas);
    for (auto& segment_group_meta : segment_group_metas) {
        Version version = _rowset_meta->version();
        int64_t version_hash = _rowset_meta->version_hash();
        std::shared_ptr<SegmentGroup> segment_group(new SegmentGroup(_rowset_meta->tablet_id(),
                _rowset_meta->rowset_id(), _tablet_schema, _num_key_fields, _num_short_key_fields,
                _num_rows_per_row_block, _rowset_path, version, version_hash,
                false, segment_group_meta.segment_group_id(), segment_group_meta.num_segments()));
        if (segment_group.get() == nullptr) {
            LOG(WARNING) << "fail to create olap segment_group. [version='" << version.first
                << "-" << version.second << "' rowset_id='" << _rowset_meta->rowset_id() << "']";
            return OLAP_ERR_CREATE_FILE_ERROR;
        }
        _segment_groups.push_back(segment_group);
        if (segment_group_meta.has_empty()) {
            segment_group->set_empty(segment_group_meta.empty());
        }

        // validate segment group
        if (segment_group->validate() != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to validate segment_group. [version="<< version.first
                    << "-" << version.second << " version_hash=" << version_hash;
                // if load segment group failed, rowset init failed
            return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
        }

        if (segment_group_meta.column_pruning_size() != 0) {
            size_t column_pruning_size = segment_group_meta.column_pruning_size();
            if (_num_key_fields != column_pruning_size) {
                LOG(ERROR) << "column pruning size is error."
                        << "column_pruning_size=" << column_pruning_size << ", "
                        << "num_key_fields=" << _num_key_fields;
                return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR; 
            }
            std::vector<std::pair<std::string, std::string>> column_statistic_strings(_num_key_fields);
            std::vector<bool> null_vec(_num_key_fields);
            for (size_t j = 0; j < _num_key_fields; ++j) {
                ColumnPruning column_pruning = segment_group_meta.column_pruning(j);
                column_statistic_strings[j].first = column_pruning.min();
                column_statistic_strings[j].second = column_pruning.max();
                if (column_pruning.has_null_flag()) {
                    null_vec[j] = column_pruning.null_flag();
                } else {
                    null_vec[j] = false;
                }
            }
            OLAPStatus status = segment_group->add_column_statistics(column_statistic_strings, null_vec);
            if (status != OLAP_SUCCESS) {
                return status;
            }

            OLAPStatus res = segment_group->load();
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to load segment_group. res=" << res << ", "
                        << "version=" << version.first << "-"
                        << version.second << ", "
                        << "version_hash=" << version_hash;
                return res; 
            }
        }
    }
    _segment_group_size = _segment_groups.size();
    if (_is_cumulative_rowset && _segment_group_size > 1) {
        LOG(WARNING) << "invalid segment group meta for cumulative rowset. segment group size:"
                << _segment_group_size;
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR; 
    }
    return OLAP_SUCCESS;
}

}  // namespace doris
