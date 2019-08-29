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

#include "olap/rowset/segment_v2/bloom_filter_page.h"

namespace doris {
namespace segment_v2 {

Status BloomFilterPageBuilder::add(const uint8_t* vals, size_t count) {
    for (int i = 0; i < count; ++i) {
        BloomKeyProbe key;
        if (vals == nullptr) {
            key.init(Slice((const char*)nullptr, 0));
        } else {

            if (_type_info->type() == OLAP_FIELD_TYPE_CHAR
                    || _type_info->type() == OLAP_FIELD_TYPE_VARCHAR
                    || _type_info->type() == OLAP_FIELD_TYPE_HLL) {
                Slice* value = (Slice*)vals;
                key.init(*value);
            } else {
                Slice value(vals, _type_info->size());
                key.init(value);
            }
        }
        _bf_builder->add_key(key);
        if (_bf_builder->num_inserted() >= _bf_builder->expected_num()) {
            flush();
        }
        if (vals != nullptr) {
            vals += sizeof(Slice);
        }
    }
    return Status::OK();
}

Status BloomFilterPageBuilder::flush() {
    uint32_t bf_size = _bf_builder->get_bf_size();
    std::unique_ptr<char> bf(new char[bf_size]);
    _bf_builder->write(bf.get());
    Slice data(bf.get(), bf_size);
    size_t num = 1;
    RETURN_IF_ERROR(_page_builder->add((const uint8_t*)&data, &num));
    LOG(INFO) << "add a new bloom filter block, bf size:" << bf_size;
    // reset the bloom filter builder
    _bf_builder->clear();
    return Status::OK();
}

Slice BloomFilterPageBuilder::finish() {
    // first flush last bloom filter block data
    if (_bf_builder->num_inserted() > 0) {
        auto st = flush();
        if (!st.ok()) {
            return Slice();
        }
    }
    
    // write BloomFilterPageFooterPB to page
    BloomFilterPageFooterPB footer;
    footer.set_hash_function_num(_bf_builder->hash_function_num());
    footer.set_expected_num(_block_size);
    footer.set_bit_size(_bf_builder->bit_size());
    LOG(INFO) << "bloom filter block size:" << _block_size << ", bit size:" << _bf_builder->bit_size() << ", hash fun num:" << _bf_builder->hash_function_num();
    std::string value;
    bool ret = footer.SerializeToString(&value);
    if (!ret) {
        return Slice((const uint8_t*)nullptr, 0);
    }
    // add BloomFilterPageFooterPB as the last entry
    size_t num = 1;
    auto st = _page_builder->add((const uint8_t*)&value, &num);
    if (!st.ok()) {
        return Slice();
    }
    return _page_builder->finish();
}

Status BloomFilterPage::load() {
    BinaryPlainPageDecoder page_decoder(_data);
    RETURN_IF_ERROR(page_decoder.init());
    size_t count = page_decoder.count();
    if (count == 0) {
        return Status::Corruption("invalid bloom filter page");
    }
    // the number of bloom filter blocks
    _block_num = count - 1;
    // last entry is BloomFilterPageFooterPB slice
    Slice footer_slice = page_decoder.string_at_index(count - 1);
    BloomFilterPageFooterPB footer;
    bool ret = footer.ParseFromString(std::string(footer_slice.data, footer_slice.size));
    if (!ret) {
        return Status::Corruption("parse BloomFilterPageFooterPB failed");
    }
    uint32_t hash_function_num = footer.hash_function_num();
    uint32_t bit_size = footer.bit_size();
    _expected_num = footer.expected_num();
    for (int i = 0; i < count -1; ++i) {
        Slice data = page_decoder.string_at_index(i);
        BloomFilter bloom_filter(data, hash_function_num, bit_size);
        RETURN_IF_ERROR(bloom_filter.load());
        _bloom_filters.emplace_back(bloom_filter);
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
