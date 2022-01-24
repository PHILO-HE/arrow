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

#include "gandiva/random_generator_holder.h"
#include "gandiva/node.h"

namespace gandiva {
Status RandomGeneratorHolder::Make(const FunctionNode& node,
                                   std::shared_ptr<RandomGeneratorHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() > 2,
                  Status::Invalid("'random' function requires at most two parameters"));

  if (node.children().size() == 0) {
    *holder = std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder());
    return Status::OK();
  }

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(0).get());
  ARROW_RETURN_IF(literal == nullptr,
                  Status::Invalid("'random' function requires a literal as parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      literal_type != arrow::Type::INT32 && literal_type != arrow::Type::INT64,
      Status::Invalid("'random' function requires an int32/int64 literal as parameter"));

  // The offset is a partition ID in spark SQL. It is used to achieve genuine random distribution globally.
  int32_t offset = 0;
  if (node.children().size() > 1) {
    auto offset_node = dynamic_cast<LiteralNode*>(node.children().at(1).get());
    offset = offset_node->is_null() ? 0 : arrow::util::get<int32_t>(offset_node->holder());
  }
  if (literal_type == arrow::Type::INT32) {
    int32_t seed = literal->is_null() ? 0 : arrow::util::get<int32_t>(literal->holder());
    *holder = std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder(seed + offset));
  } else {
    int64_t seed = literal->is_null() ? 0 : arrow::util::get<int64_t>(literal->holder());
    *holder = std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder(
      seed + static_cast<int64_t>(offset)));
  }
  return Status::OK();
}
}  // namespace gandiva
