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

#pragma once

#include <sys/types.h>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <ostream>
#include <glog/logging.h>

#include "util/coding.h"
#include "util/faststring.h"
#include "gutil/port.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/bitshuffle_wrapper.h"

namespace doris {
namespace segment_v2 {

void abort_with_bitshuffle_error(int64_t val);

// BitshufflePageBuilder bitshuffles and compresses the bits of fixed
// size type blocks with lz4.
//
// The page format is as follows:
//
// 1. Header: (20 bytes total)
//
//    <first_ordinal> [32-bit]
//      The ordinal offset of the first element in the page.
//
//    <num_elements> [32-bit]
//      The number of elements encoded in the page.
//
//    <compressed_size> [32-bit]
//      The post-compression size of the page, including this header.
//
//    <padded_num_elements> [32-bit]
//      Padding is needed to meet the requirements of the bitshuffle
//      library such that the input/output is a multiple of 8. Some
//      ignored elements are appended to the end of the page if necessary
//      to meet this requirement.
//
//      This header field is the post-padding element count.
//
//    <elem_size_bytes> [32-bit]
//      The size of the elements, in bytes, as actually encoded. In the
//      case that all of the data in a page can fit into a smaller
//      integer type, then we may choose to encode that smaller type
//      to save CPU costs.
//
//      This is currently only implemented in the UINT32 page type.
//
//   NOTE: all on-disk ints are encoded little-endian
//
// 2. Element data
//
//    The header is followed by the bitshuffle-compressed element data.
//
template<FieldType Type>
class BitshufflePageBuilder : public PageBuilder {
public:
    BitshufflePageBuilder(const BuilderOptions* options) :
            _options(options),
            _count(0),
            _remain_element_capacity(0),
            _finished(false) {
        reset();
    }

    bool is_page_full() override {
        return _remain_element_capacity == 0;
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        int to_add = std::min<int>(_remain_element_capacity, *count);
        _data.append(vals, to_add * SIZE_OF_TYPE);
        _count += to_add;
        _remain_element_capacity -= to_add;
        // return added number through count
        *count = to_add;
        return Status::OK;
    }

    Slice finish(rowid_t page_first_rowid) override {
        return _finish(page_first_rowid, SIZE_OF_TYPE);
    }

    void reset() override {
        auto block_size = _options->data_page_size;
        _count = 0;
        _data.clear();
        _data.reserve(block_size);
        DCHECK_EQ(reinterpret_cast<uintptr_t>(_data.data()) & (alignof(CppType) - 1), 0)
            << "buffer must be naturally-aligned";
        _buffer.clear();
        _buffer.resize(HEADER_SIZE);
        _finished = false;
        _remain_element_capacity = block_size / SIZE_OF_TYPE;
    }

    size_t count() const {
        return _count;
    }

    Status get_dictionary_page(Slice* dictionary_page) override {
        return Status("NOT_IMPLEMENTED");
    }

    Status get_bitmap_page(Slice* bitmap_page) override {
        return Status("NOT_IMPLEMENTED");
    }

private:
    Slice _finish(rowid_t page_first_rowid, int final_size_of_type) {
        _data.resize(HEADER_SIZE + final_size_of_type * _count);

        // Do padding so that the input num of element is multiple of 8.
        int num_elems_after_padding = ALIGN_UP(_count, 8);
        int padding_elems = num_elems_after_padding - _count;
        int padding_bytes = padding_elems * final_size_of_type;
        for (int i = 0; i < padding_bytes; i++) {
            _data.push_back(0);
        }

        _buffer.resize(HEADER_SIZE +
                bitshuffle::compress_lz4_bound(num_elems_after_padding, final_size_of_type, 0));

        encode_fixed32_le(&_buffer[0], page_first_rowid);
        encode_fixed32_le(&_buffer[4], _count);
        int64_t bytes = bitshuffle::compress_lz4(_data.data(), &_buffer[HEADER_SIZE],
                num_elems_after_padding, final_size_of_type, 0);
        if (PREDICT_FALSE(bytes < 0)) {
            // This means the bitshuffle function fails.
            // Ideally, this should not happen.
            abort_with_bitshuffle_error(bytes);
            // It does not matter what will be returned here,
            // since we have logged fatal in abort_with_bitshuffle_error().
            return Slice();
        }
        encode_fixed32_le(&_buffer[8], HEADER_SIZE + bytes);
        encode_fixed32_le(&_buffer[12], num_elems_after_padding);
        encode_fixed32_le(&_buffer[16], final_size_of_type);
        _finished = true;
        return Slice(_buffer.data(), HEADER_SIZE + bytes);
    }

    typedef typename TypeTraits<Type>::CppType CppType;

    CppType cell(int idx) const {
        DCHECK_GE(idx, 0);
        CppType ret;
        memcpy(&ret, &_data[idx * SIZE_OF_TYPE], sizeof(CppType));
        return ret;
    }

    // Length of a header.
    static const size_t HEADER_SIZE = sizeof(uint32_t) * 5;
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };
    const BuilderOptions* _options;
    uint32_t _count;
    int _remain_element_capacity;
    bool _finished;
    faststring _data;
    faststring _buffer;
};

template<FieldType Type>
class BitShufflePageDecoder : public PageDecoder {
    public:
        BitShufflePageDecoder(Slice data) : _data(data),
        _parsed(false),
        _page_first_ordinal(0),
        _num_elements(0),
        _compressed_size(0),
        _num_element_after_padding(0),
        _size_of_element(0),
        _cur_index(0) { }

        Status init() override {
            CHECK(!_parsed);
            if (_data.size < HEADER_SIZE) {
                std::stringstream ss;
                ss << "file corrupton: invalid data size:" << _data.size << ", header size:" << HEADER_SIZE;
                return Status(ss.str());
            }
            _page_first_ordinal = decode_fixed32_le((const uint8_t*)&_data[0]);
            _num_elements = decode_fixed32_le((const uint8_t*)&_data[4]);
            _compressed_size   = decode_fixed32_le((const uint8_t*)&_data[8]);
            if (_compressed_size != _data.size) {
                std::stringstream ss;
                ss << "Size information unmatched, _compressed_size:" << _compressed_size
                    << ", data size:" << _data.size;
                return Status(ss.str());
            }
            _num_element_after_padding = decode_fixed32_le((const uint8_t*)&_data[12]);
            if (_num_element_after_padding != ALIGN_UP(_num_elements, 8)) {
                std::stringstream ss;
                ss << "num of element information corrupted,"
                    << " _num_element_after_padding:" << _num_element_after_padding
                    << ", _num_elements:" << _num_elements;
                return Status(ss.str());
            }
            _size_of_element = decode_fixed32_le((const uint8_t*)&_data[16]);
            switch (_size_of_element) {
                case 1:
                case 2:
                case 4:
                case 8:
                case 16:
                    break;
                default:
                    std::stringstream ss;
                ss << "invalid size_of_elem:" << _size_of_element;
                return Status(ss.str());
        }

        // Currently, only the UINT32 block encoder supports expanding size:
        if (UNLIKELY(Type != OLAP_FIELD_TYPE_UNSIGNED_INT && _size_of_element != SIZE_OF_TYPE)) {
            std::stringstream ss;
            ss << "invalid size info. size of element:" << _size_of_element
                << ", SIZE_OF_TYPE:" << SIZE_OF_TYPE
                << ", type:" << Type;
            return Status(ss.str());
        }
        if (UNLIKELY(_size_of_element > SIZE_OF_TYPE)) {
            std::stringstream ss;
            ss << "invalid size info. size of element:" << _size_of_element
                << ", SIZE_OF_TYPE:" << SIZE_OF_TYPE;
            return Status(ss.str());
        }

        RETURN_IF_ERROR(_decode());
        _parsed = true;
        return Status::OK;
    }

    Status seek_to_position_in_page(size_t pos) override {
        DCHECK(_parsed) << "Must call init()";
        if (PREDICT_FALSE(_num_elements == 0)) {
            DCHECK_EQ(0, pos);
            return Status("invalid pos");
        }

        DCHECK_LE(pos, _num_elements);
        _cur_index = pos;
        return Status::OK;
    }

    Status next_batch(size_t* n, ColumnVectorView* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK;
        }

        size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elements - _cur_index));
        _copy_next_values(max_fetch, dst->column_vector()->col_data());
        *n = max_fetch;
        _cur_index += max_fetch;

        return Status::OK;
    }

    size_t count() const override {
        return _num_elements;
    }

    size_t current_index() const override {
        return _cur_index;
    }

    rowid_t get_first_rowid() const override {
        return _page_first_ordinal;
    }

private:
    void _copy_next_values(size_t n, void* data) {
        memcpy(data, &_decoded[_cur_index * SIZE_OF_TYPE], n * SIZE_OF_TYPE);
    }

    Status _decode() {
        if (_num_elements > 0) {
            int64_t bytes;
            _decoded.resize(_num_element_after_padding * _size_of_element);
            char* in = const_cast<char*>(&_data[HEADER_SIZE]);
            bytes = bitshuffle::decompress_lz4(in, _decoded.data(), _num_element_after_padding,
                    _size_of_element, 0);
            if (PREDICT_FALSE(bytes < 0)) {
                // Ideally, this should not happen.
                abort_with_bitshuffle_error(bytes);
                return Status(TStatusCode::RUNTIME_ERROR, "Unshuffle Process failed", true);
            }
        }
        return Status::OK;
    }

    typedef typename TypeTraits<Type>::CppType CppType;

    // Length of a header.
    static const size_t HEADER_SIZE = sizeof(uint32_t) * 5;
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };

    Slice _data;
    bool _parsed;
    rowid_t _page_first_ordinal;
    size_t _num_elements;
    size_t _compressed_size;
    size_t _num_element_after_padding;

    int _size_of_element;
    size_t _cur_index;
    faststring _decoded;
};

} // namespace segment_v2
} // namespace doris
