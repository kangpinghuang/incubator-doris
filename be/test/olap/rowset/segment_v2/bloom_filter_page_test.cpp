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

#include <gtest/gtest.h>
#include <iostream>

#include "common/logging.h"

namespace doris {
namespace segment_v2 {

class BloomFilterPageTest : public testing::Test {
public:
    BloomFilterPageTest() { }
    virtual ~BloomFilterPageTest() {
    }
};

TEST_F(BloomFilterPageTest, normal) {
    TypeInfo* type_info = get_type_info(OLAP_FIELD_TYPE_VARCHAR);
    BloomFilterPageBuilder bf_page_builder(type_info, 2, 0.05);
    std::string bytes;
    // first block
    Slice slice1("hello");
    bf_page_builder.add((const uint8_t*)&slice1, 1);
    Slice slice2("doris");
    bf_page_builder.add((const uint8_t*)&slice2, 1);
    // second block
    Slice slice3("hi");
    bf_page_builder.add((const uint8_t*)&slice3, 1);
    Slice slice4("world");
    bf_page_builder.add((const uint8_t*)&slice4, 1);

    Slice bf_data = bf_page_builder.finish();
    BloomFilterPage bf_page(bf_data);
    auto st = bf_page.load();
    ASSERT_TRUE(st.ok()) << "st:" << st.to_string();
    ASSERT_EQ(2, bf_page.block_num());
    ASSERT_EQ(2, bf_page.expected_num());

    const BloomFilter& bf_1 = bf_page.get_bloom_filter(0);
    BloomKeyProbe key;
    key.init(Slice("hello"));
    ASSERT_TRUE(bf_1.check_key(key));
    key.init(Slice("doris"));
    ASSERT_TRUE(bf_1.check_key(key));
    key.init(Slice("hi"));
    ASSERT_FALSE(bf_1.check_key(key));
    
    const BloomFilter& bf_2 = bf_page.get_bloom_filter(1);
    key.init(Slice("hi"));
    ASSERT_TRUE(bf_2.check_key(key));
    key.init(Slice("world"));
    ASSERT_TRUE(bf_2.check_key(key));
    key.init(Slice("hello"));
    ASSERT_FALSE(bf_2.check_key(key));
}

TEST_F(BloomFilterPageTest, corrupt) {
    std::string str;
    str.resize(4);

    encode_fixed32_le((uint8_t*)str.data(), 1);

    Slice slice(str);
    BloomFilterPage bf_page(slice);
    auto st = bf_page.load();
    ASSERT_FALSE(st.ok());
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

