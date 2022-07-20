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
// #include <iostream>

using namespace simdjson;

namespace gandiva {

//using namespace simdjson;

Status JsonHolder::Make(const FunctionNode& node, std::shared_ptr<JsonHolder>* holder) {
  return Make(holder);
}

Status JsonHolder::Make(std::shared_ptr<JsonHolder>* holder) {
  *holder = std::shared_ptr<JsonHolder>(new JsonHolder());
  return Status::OK();
}

// const uint8_t* JsonHolder::operator()(gandiva::ExecutionContext* ctx, const std::string& json_str, const std::string& json_path, int32_t* out_len) {
//   std::unique_ptr<arrow::json::BlockParser> parser;
//   (arrow::json::BlockParser::Make(parse_options_, &parser));
//   (parser->Parse(std::make_shared<arrow::Buffer>(json_str)));
//   std::shared_ptr<arrow::Array> parsed;
//   (parser->Finish(&parsed));
//   auto struct_parsed = std::dynamic_pointer_cast<arrow::StructArray>(parsed);
//   //json_path example: $.col_14, will extract col_14 here
//   if (json_path.length() < 3) {
//     return nullptr;
//   }
//   auto col_name = json_path.substr(2);
//   // illegal json string.
//   if (struct_parsed == nullptr) {
//     return nullptr;
//   }
//   auto dict_parsed = std::dynamic_pointer_cast<arrow::DictionaryArray>(
//       struct_parsed->GetFieldByName(col_name));
//   // no data contained for given field.
//   if (dict_parsed == nullptr) {
//     return nullptr;
//   }
//   auto dict_array = dict_parsed->dictionary();
//   // needs to see whether there is a case that has more than one indices.
//   auto res_index = dict_parsed->GetValueIndex(0);
//   auto utf8_array = std::dynamic_pointer_cast<arrow::BinaryArray>(dict_array);
//   auto res = utf8_array->GetValue(res_index, out_len);
//   // empty string case.
//   if (*out_len == 0) {
//     return reinterpret_cast<const uint8_t*>("");
//   }
//   uint8_t* result_buffer = reinterpret_cast<uint8_t*>(ctx->arena()->Allocate(*out_len));
//   memcpy(result_buffer, std::string((char*)res, *out_len).data(), *out_len);
//   return result_buffer;
// }

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
   case ondemand::json_type::number:
     double num_res;
     error = raw_res.get_double().get(num_res);
    //  std::cout << "num_res: " << num_res << std::endl;
     if (!error) {
       res = std::string_view(std::to_string(num_res));
       //ondemand::object o;
       //res = std::string_view(to_string(raw_res.get_object().get(o)));
     }
     break;
   case ondemand::json_type::string:
     //std::string_view res;
     error = raw_res.get_string().get(res);
     break;
   case ondemand::json_type::object:
     // Not supported.
     return nullptr;
   case ondemand::json_type::array:
     // Not supported.
     return nullptr;
   case ondemand::json_type::null:
     return nullptr;
  }
  //auto raw_res = doc.find_field(field_name);
  //auto res = std::string_view(raw_res);
  //std::string_view res;
  //auto error = raw_res.get_string().get(res);
  //std::cout << error << std::endl;
  if (error) {
   return nullptr;
  }
  //std::string_view xyz;
  //auto res = doc.find_field(field_name).raw_json_token().get(&xyz);
  *out_len = res.length();
  uint8_t* result_buffer = reinterpret_cast<uint8_t*>(ctx->arena()->Allocate(*out_len));
  memcpy(result_buffer, res.data(), *out_len);
  return result_buffer;
 }

}  // namespace gandiva
