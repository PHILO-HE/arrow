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

#include <iostream>
#include <regex>

#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {

Status JsonHolder::Make(const FunctionNode& node, std::shared_ptr<JsonHolder>* holder) {
  return Make(holder);
}

Status JsonHolder::Make(std::shared_ptr<JsonHolder>* holder) {
  *holder = std::shared_ptr<JsonHolder>(new JsonHolder());
  return Status::OK();
}

const uint8_t* JsonHolder::operator()(gandiva::ExecutionContext* ctx, const std::string& json_str, const std::string& json_path, 
int32_t* out_len, bool in_valid, bool* out_valid) {
   *out_valid = true;
   if (!in_valid) {
      *out_valid = false;
      return nullptr;
    }  
 
  std::unique_ptr<arrow::json::BlockParser> parser;
  (arrow::json::BlockParser::Make(parse_options_, &parser));

  (parser->Parse(std::make_shared<arrow::Buffer>(json_str)));
  std::shared_ptr<arrow::Array> parsed;
  (parser->Finish(&parsed));
  auto struct_parsed = std::dynamic_pointer_cast<arrow::StructArray>(parsed);
  //json_path example: $.col_14, will extract col_14 here
  // needs to gurad failure here
  if (json_path.length() < 3) {
    *out_valid = false;
    return nullptr;
  }
  auto col_name = json_path.substr(2);
  // illegal json string.
  if (struct_parsed == nullptr) {
    *out_valid = false;
    return nullptr;
  }
  auto dict_parsed = std::dynamic_pointer_cast<arrow::DictionaryArray>(
      struct_parsed->GetFieldByName(col_name));
  // no data contained for given field.
  if (dict_parsed == nullptr) {
    *out_valid = false;
    return nullptr;
  }
  auto dict_array = dict_parsed->dictionary();
  auto utf8_array = std::dynamic_pointer_cast<arrow::BinaryArray>(dict_array);
  auto res = utf8_array->GetValue(0, out_len);

  uint8_t* result_buffer = reinterpret_cast<uint8_t*>(ctx->arena()->Allocate(*out_len));
  memcpy(result_buffer, std::string((char*)res, *out_len).data(), *out_len);
  return result_buffer;
}

}  // namespace gandiva
