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

#include "gandiva/extract_holder.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace gandiva {

  class TestExtractHolder : public ::testing::Test {
  protected:
      ExecutionContext execution_context_;
  };

  TEST_F(TestExtractHolder, TestRegexpExtract) {
    std::shared_ptr<ExtractHolder> extract_holder;
    auto status = ExtractHolder::Make("(\\d+)-(\\d+)", &extract_holder);
    EXPECT_EQ(status.ok(), true) << status.message();
    std::string input_string = "100-200";
    int32_t out_length = 0;

    auto &extract = *extract_holder;
    // The regex group index is 1.
    const char *ret1 = extract(&execution_context_, input_string.c_str(),
                              static_cast<int32_t>(input_string.length()), 1, &out_length);
    std::string ret1_as_str(ret1, out_length);
    EXPECT_EQ(out_length, 3);
    EXPECT_EQ(ret1_as_str, "100");

    // The regex group index is 2.
    const char *ret2 = extract(&execution_context_, input_string.c_str(),
                   static_cast<int32_t>(input_string.length()), 2, &out_length);
    std::string ret2_as_str(ret2, out_length);
    EXPECT_EQ(out_length, 3);
    EXPECT_EQ(ret2_as_str, "200");

    // Partial match case.
    input_string = "a-100-200-b";
    const char *ret3 = extract(&execution_context_, input_string.c_str(),
                             static_cast<int32_t>(input_string.length()), 1, &out_length);
    std::string ret3_as_str(ret3, out_length);
    EXPECT_EQ(out_length, 3);
    EXPECT_EQ(ret3_as_str, "100");

  // Not match case.
    input_string = "abc-abc";
    const char *ret4 = extract(&execution_context_, input_string.c_str(),
                             static_cast<int32_t>(input_string.length()), 1, &out_length);
    std::string ret4_as_str(ret4, out_length);
    EXPECT_EQ(out_length, 0);
    EXPECT_EQ(ret4_as_str, "");
}
}  // namespace gandiva
