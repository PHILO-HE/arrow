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

#include "gandiva/json_holder.h"

#include <regex>

#include "gandiva/node.h"
#include "gandiva/regex_util.h"
#include <sstream>
#include <iostream>

using namespace simdjson;

namespace gandiva {

Status JsonHolder::Make(const FunctionNode& node, std::shared_ptr<JsonHolder>* holder) {
  return Make(holder);
}

Status JsonHolder::Make(std::shared_ptr<JsonHolder>* holder) {
  *holder = std::shared_ptr<JsonHolder>(new JsonHolder());
  return Status::OK();
}

const uint8_t* JsonHolder::operator()(gandiva::ExecutionContext* ctx, const std::string& json_str,
 const std::string& json_path, int32_t* out_len) {
  padded_string padded_input(json_str);
  ondemand::document doc;
  try {
    doc = parser_->iterate(padded_input);
  } catch (simdjson_error& e) {
    return nullptr;
  }
  if (json_path.length() < 3) {
    return nullptr;
  }
  auto field_name = json_path.substr(2);
  auto raw_res = doc.find_field(field_name);
  // std::cout << raw_res.type() << std::endl;
  error_code error;
  std::string_view res;
  switch (raw_res.type()) {
   case ondemand::json_type::number: {
      std::stringstream ss;
      double num_res;
      error = raw_res.get_double().get(num_res);
      if (!error) {
        ss << num_res;
        res = ss.str();
      }
      break;
    }
   case ondemand::json_type::string:
     //std::string_view res;
     error = raw_res.get_string().get(res);
     break;
   case ondemand::json_type::boolean:
     // Not supported.
     return nullptr;
   case ondemand::json_type::object: {
     // Not supported.
     auto obj = raw_res.get_object();
     std::cout << "object: " << std::endl;
     std::cout << obj["hello"] << std::endl;
     return nullptr;
    }
   case ondemand::json_type::array:
     // Not supported.
     return nullptr;
   case ondemand::json_type::null:
     return nullptr;
  }
  std::cout << "error: " << error << std::endl;
  if (error) {
   return nullptr;
  }
  *out_len = res.length();
  uint8_t* result_buffer = reinterpret_cast<uint8_t*>(ctx->arena()->Allocate(*out_len));
  memcpy(result_buffer, res.data(), *out_len);
  return result_buffer;
 }

}  // namespace gandiva
