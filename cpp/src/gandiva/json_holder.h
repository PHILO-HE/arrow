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

#include <memory>
#include <string>

#include "arrow/json/api.h"
#include "arrow/json/parser.h"
#include "arrow/status.h"
#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/simdjson.h"
#include "gandiva/visibility.h"

using namespace simdjson;

namespace gandiva {

/// Function Holder for SQL 'get_json_object'
class GANDIVA_EXPORT JsonHolder : public FunctionHolder {
 public:
  JsonHolder() {
   parser_ = std::make_shared<ondemand::parser>();
  }
  ~JsonHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<JsonHolder>* holder);
  static Status Make(std::shared_ptr<JsonHolder>* holder);

  //TODO(): should try to return const uint8_t *
  const uint8_t* operator()(ExecutionContext* ctx, const std::string& json_str, const std::string& json_path, int32_t* out_len);
  
  std::shared_ptr<ondemand::parser> parser_;
};

}  // namespace gandiva