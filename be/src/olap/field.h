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

#ifndef DORIS_BE_SRC_OLAP_FIELD_H
#define DORIS_BE_SRC_OLAP_FIELD_H

#include <string>
#include <sstream>

#include "olap/aggregate_func.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/key_coder.h"
#include "olap/utils.h"
#include "olap/row_cursor_cell.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/mem_util.hpp"
#include "util/slice.h"

namespace doris {

// A Field is used to represent a column in memory format.
// User can use this class to access or deal with column data in memory.
class Field {
public:
    static Field* create(const TabletColumn& column) {
        return new Field(column);
    }

    static Field* create_by_type(const FieldType& type) {
        // create by type
        return new Field(type);
    }

    Field(const TabletColumn& column)
        : _type_info(get_type_info(column.type())),
        _agg_info(get_aggregate_info(column.aggregation(), column.type())),
        _key_coder(get_key_coder(column.type())),
        _index_size(column.index_length()),
        _is_nullable(column.is_nullable()) { }

    Field(FieldType type)
        : _type_info(get_type_info(type)),
        _agg_info(get_aggregate_info(OLAP_FIELD_AGGREGATION_NONE, type)),
        _key_coder(get_key_coder(type)),
        _index_size(_type_info->size()),
        _is_nullable(true) {
    }

    Field(const FieldAggregationMethod& agg, const FieldType& type, bool is_nullable)
        : _type_info(get_type_info(type)),
        _agg_info(get_aggregate_info(agg, type)),
        _key_coder(get_key_coder(type)),
        _index_size(-1),
        _is_nullable(is_nullable) {
    }

    Field(const FieldAggregationMethod& agg, const FieldType& type, size_t index_size, bool is_nullable)
        : _type_info(get_type_info(type)),
        _agg_info(get_aggregate_info(agg, type)),
        _key_coder(get_key_coder(type)),
        _index_size(index_size),
        _is_nullable(is_nullable) {
    }

    inline size_t size() const { return _type_info->size(); }
    inline size_t field_size() const { return size() + 1; }
    inline size_t index_size() const { return _index_size; }

    inline void set_to_max(char* buf) const { return _type_info->set_to_max(buf); }
    inline void set_to_min(char* buf) const { return _type_info->set_to_min(buf); }

    inline void agg_update(RowCursorCell* dest, const RowCursorCell& src, Arena* arena = nullptr) const {
        _agg_info->update(dest, src, arena);
    }

    void agg_finalize(RowCursorCell* dst, Arena* arena = nullptr) const {
        _agg_info->finalize(dst->mutable_cell_ptr(), arena);
    }

    // Test if these two cell is equal with each other
    template<typename LhsCellType, typename RhsCellType>
    bool equal(const LhsCellType& lhs,
               const RhsCellType& rhs) const {
        bool l_null = lhs.is_null();
        bool r_null = rhs.is_null();

        if (l_null != r_null) {
            return false;
        } else if (l_null) {
            return true;
        } else {
            return _type_info->equal(lhs.cell_ptr(), rhs.cell_ptr());
        }
    }

    // Only compare column content, without considering NULL condition.
    // RETURNS:
    //      0 means equal,
    //      -1 means left less than rigth,
    //      1 means left bigger than right
    int compare(const void* left, const void* right) const {
        return _type_info->cmp(left, right);
    }

    // Compare two types of cell.
    // This function differs compare in that this function compare cell which
    // will consider the condition which cell may be NULL. While compare only
    // compare column content without considering NULL condition.
    // Only compare column content, without considering NULL condition.
    // RETURNS:
    //      0 means equal,
    //      -1 means left less than rigth,
    //      1 means left bigger than right
    template<typename LhsCellType, typename RhsCellType>
    int compare_cell(const LhsCellType& lhs,
                     const RhsCellType& rhs) const {
        bool l_null = lhs.is_null();
        bool r_null = rhs.is_null();
        if (l_null != r_null) {
            return l_null ? -1 : 1;
        }
        return l_null ? 0 : _type_info->cmp(lhs.cell_ptr(), rhs.cell_ptr());
    }

    // Used to compare short key index. Because short key will truncate
    // a varchar column, this function will handle in this condition.
    template<typename LhsCellType, typename RhsCellType> 
    inline int index_cmp(const LhsCellType& lhs, const RhsCellType& rhs) const;

    // Copy source cell's content to destination cell directly.
    // For string type, this function assume that destination has
    // enough space and copy source content into destination without
    // memory allocation.
    template<typename DstCellType, typename SrcCellType>
    void direct_copy(DstCellType* dst, const SrcCellType& src) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        return _type_info->direct_copy(dst->mutable_cell_ptr(), src.cell_ptr());
    }

    // deep copy source cell' content to destination cell.
    // For string type, this will allocate data form pool,
    // and copy srouce's conetent.
    template<typename DstCellType, typename SrcCellType>
    void deep_copy(DstCellType* dst,
                   const SrcCellType& src,
                   MemPool* pool) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        _type_info->deep_copy(dst->mutable_cell_ptr(), src.cell_ptr(), pool);
    }

    template<typename DstCellType, typename SrcCellType>
    void deep_copy(DstCellType* dst,
                   const SrcCellType& src,
                   Arena* arena) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        _type_info->copy_with_arena(dst->mutable_cell_ptr(), src.cell_ptr(), arena);
    }

    // This function will initialize destination with source.
    // This functionn differs copy functionn in that if this filed
    // contain aggregate information, this functionn will initialize
    // destination in aggregate format, and update with srouce content.
    template<typename DstCellType, typename SrcCellType>
    void agg_init(DstCellType* dst, const SrcCellType& src) const;

    // deep copy filed content from `src` to `dst` without null-byte
    inline void deep_copy_content(char* dst, const char* src, MemPool* mem_pool) const {
        _type_info->deep_copy(dst, src, mem_pool);
    }

    // shallow copy filed content from `src` to `dst` without null-byte.
    // for string like type, shallow copy only copies Slice, not the actual data pointed by slice.
    inline void shallow_copy_content(char* dst, const char* src) const {
        _type_info->shallow_copy(dst, src);
    }

    // copy filed content from src to dest without nullbyte
    inline void copy_content(char* dest, const char* src, Arena* arena) const {
        _type_info->copy_with_arena(dest, src, arena);
    }

    // Copy srouce content to destination in index format.
    template<typename DstCellType, typename SrcCellType>
    void to_index(DstCellType* dst, const SrcCellType& src) const;

    // used by init scan key stored in string format
    // value_string should end with '\0'
    inline OLAPStatus from_string(char* buf, const std::string& value_string) const {
        return _type_info->from_string(buf, value_string);
    }

    // 将内部的value转成string输出
    // 没有考虑实现的性能，仅供DEBUG使用
    inline std::string to_string(char* src) const {
        return _type_info->to_string(src);
    }

    template<typename CellType>
    std::string debug_string(const CellType& cell) const {
        std::stringstream ss;
        if (cell.is_null()) {
            ss << "(null)";
        } else {
            ss << _type_info->to_string(cell.cell_ptr());
        }
        return ss.str();
    }


    template<typename CellType>
    uint32_t hash_code(const CellType& cell, uint32_t seed) const;

    FieldType type() const { return _type_info->type(); }
    const TypeInfo* type_info() const { return _type_info; }
    bool is_nullable() const { return _is_nullable; }

    void encode_ascending(const void* value, std::string* buf) const {
        _key_coder->encode_ascending(value, _index_size, buf);
    }
    
    Status decode_ascending(Slice* encoded_key, uint8_t* cell_ptr, Arena* arena) const {
        return _key_coder->decode_ascending(encoded_key, _index_size, cell_ptr, arena);
    }
private:
    // Field的最大长度，单位为字节，通常等于length， 变长字符串不同
    const TypeInfo* _type_info;
    const AggregateInfo* _agg_info;
    const KeyCoder* _key_coder;
    uint16_t _index_size;
    bool _is_nullable;
};

template<typename LhsCellType, typename RhsCellType>
int Field::index_cmp(const LhsCellType& lhs, const RhsCellType& rhs) const {
    bool l_null = lhs.is_null();
    bool r_null = rhs.is_null();
    if (l_null != r_null) {
        return l_null ? -1 : 1;
    } else if (l_null){
        return 0;
    }

    int32_t res = 0;
    if (type() == OLAP_FIELD_TYPE_VARCHAR) {
        const Slice* l_slice = reinterpret_cast<const Slice*>(lhs.cell_ptr());
        const Slice* r_slice = reinterpret_cast<const Slice*>(rhs.cell_ptr());

        if (r_slice->size + OLAP_STRING_MAX_BYTES > _index_size
                || l_slice->size + OLAP_STRING_MAX_BYTES > _index_size) {
            // 如果field的实际长度比short key长，则仅比较前缀，确保相同short key的所有block都被扫描，
            // 否则，可以直接比较short key和field
            int compare_size = _index_size - OLAP_STRING_MAX_BYTES;
            // l_slice size and r_slice size may be less than compare_size
            // so calculate the min of the three size as new compare_size
            compare_size = std::min(std::min(compare_size, (int)l_slice->size), (int)r_slice->size);

            // This functionn is used to compare prefix index.
            // Only the fixed length of prefix index should be compared.
            // If r_slice->size > l_slice->size, igonre the extra parts directly.
            res = strncmp(l_slice->data, r_slice->data, compare_size);
            if (res == 0 && compare_size != (_index_size - OLAP_STRING_MAX_BYTES)) {
                if (l_slice->size < r_slice->size) {
                    res = -1;
                } else if (l_slice->size > r_slice->size) {
                    res = 1;
                } else {
                    res = 0;
                }
            }
        } else {
            res = l_slice->compare(*r_slice);
        }
    } else {
        res = _type_info->cmp(lhs.cell_ptr(), rhs.cell_ptr());
    }

    return res;
}

template<typename DstCellType, typename SrcCellType>
void Field::agg_init(DstCellType* dst, const SrcCellType& src) const {
    // TODO(zc): This function is also used to initialize key columns.
    // So, refactor this in later PR
    if (OLAP_LIKELY(type() != OLAP_FIELD_TYPE_HLL)) {
        direct_copy(dst, src);
    } else {
        bool is_null = src.is_null();
        // TODO(zc): If source is null, can we set this to null?
        // I'm not sure, just set like old code
        dst->set_is_null(is_null);

        Slice* slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        size_t hll_ptr = *(size_t*)(slice->data - sizeof(HllContext*));
        HllContext* context = (reinterpret_cast<HllContext*>(hll_ptr));
        HllSetHelper::init_context(context);
#if 0
        if (is_null) {
            return;
        }
#endif
        HllSetHelper::fill_set((const char*)src.cell_ptr(), context);
        context->has_value = true;
    }
}

template<typename DstCellType, typename SrcCellType>
void Field::to_index(DstCellType* dst, const SrcCellType& src) const {
    bool is_null = src.is_null();
    dst->set_is_null(is_null);
    if (is_null) {
        return;
    }

    if (type() == OLAP_FIELD_TYPE_VARCHAR) {
        // 先清零，再拷贝
        memset(dst->mutable_cell_ptr(), 0, _index_size);
        const Slice* slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        size_t copy_size = slice->size < _index_size - OLAP_STRING_MAX_BYTES ?
                           slice->size : _index_size - OLAP_STRING_MAX_BYTES;
        *reinterpret_cast<StringLengthType*>(dst->mutable_cell_ptr()) = copy_size;
        memory_copy((char*)dst->mutable_cell_ptr() + OLAP_STRING_MAX_BYTES, slice->data, copy_size);
    } else if (type() == OLAP_FIELD_TYPE_CHAR) {
        // 先清零，再拷贝
        memset(dst->mutable_cell_ptr(), 0, _index_size);
        const Slice* slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        memory_copy(dst->mutable_cell_ptr(), slice->data, _index_size);
    } else {
        memory_copy(dst->mutable_cell_ptr(), src.cell_ptr(), size());
    }
}

template<typename CellType>
uint32_t Field::hash_code(const CellType& cell, uint32_t seed) const {
    bool is_null = cell.is_null();
    if (is_null) {
        return HashUtil::hash(&is_null, sizeof(is_null), seed);
    }
    return _type_info->hash_code(cell.cell_ptr(), seed);
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_FIELD_H
