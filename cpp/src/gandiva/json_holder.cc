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
#include <algorithm>

using namespace simdjson;

namespace gandiva {

Status JsonHolder::Make(const FunctionNode& node, std::shared_ptr<JsonHolder>* holder) {
  return Make(holder);
}

Status JsonHolder::Make(std::shared_ptr<JsonHolder>* holder) {
  *holder = std::shared_ptr<JsonHolder>(new JsonHolder());
  return Status::OK();
}

error_code handle_types(simdjson_result<ondemand::value> raw_res, std::string* res) {
 switch (raw_res.type()) {
   case ondemand::json_type::number: {
      std::stringstream ss;
      double num_res;
      auto error = raw_res.get_double().get(num_res);
      if (!error) {
        ss << num_res;
        *res = ss.str();
      }
      return error;
    }
   case ondemand::json_type::string: {
     std::string_view res_view;
     auto error = raw_res.get_string().get(res_view);
     *res = std::string(res_view);
     return error;
    }
   case ondemand::json_type::boolean: {
     bool bool_res = false;
     raw_res.get_bool().get(bool_res);
     if (bool_res) {
       *res = "true";
     } else {
       *res = "false";
     }
     return error_code::SUCCESS;
    }
   case ondemand::json_type::object: {
     // For nested case, e.g., for "{"my": {"hello": 10}}", ".$my" will return an object type.
     auto obj = raw_res.get_object();
     // For the case that result is a json object.
     std::stringstream ss;
     ss << obj;
     *res = ss.str();
     return error_code::SUCCESS;
    }
   case ondemand::json_type::array: {
     auto array_obj = raw_res.get_array();
     // For the case that result is a json object.
     std::stringstream ss;
     ss << array_obj;
     *res = ss.str();
     return error_code::SUCCESS;
    }
   case ondemand::json_type::null: {
     return error_code::UNSUPPORTED_ARCHITECTURE;
    }
  }
}

bool check_char(const std::string& raw_json_str, int check_index) {
  if (check_index > raw_json_str.length() - 1) {
    return false;
  }
  char ending_char = raw_json_str[check_index];
  if (ending_char == ',') {
    return true;
  } else if (ending_char == '}') {
    return true;
  } else if (ending_char == ' ') {
    // space can precede valid ending char.
    return check_char(raw_json_str, check_index + 1);
  } else {
    return false;
  }
}

bool is_valid(const std::string& raw_json_str, const std::string& output) {
  auto index = raw_json_str.find(output);
  if (index == std::string::npos) {
    std::runtime_error unexpected_error("Fix me! The result should be a part of json string!");
    throw unexpected_error;
  }
  // get_json_object(, $)
  if (index == 0) {
    return true;
  }
  int begin_check_index;
  // The output string may be surrounded by " in the original json string.
  if (raw_json_str[index - 1] == '\"') {
    begin_check_index = index + output.length() + 1;
  } else {
    begin_check_index = index + output.length();
  }
  return check_char(raw_json_str, begin_check_index);
}

const uint8_t* JsonHolder::operator()(gandiva::ExecutionContext* ctx, const std::string& json_str,
 const std::string& json_path, int32_t* out_len) {
  padded_string padded_input(json_str);

  // Just for json string validation. With ondemand api, when a target field is found, the remaining
  // json string will not be parsed and validated. So we use the below dom api for fully parsing and
  // return null result finally for illegal json string, which is consistent with Spark.
  // dom::parser parser_validate;
  // dom::element doc_validate;
  // auto error_validate = parser_validate.parse(padded_input).get(doc_validate);
  // if (error_validate) {
  //   return nullptr;
  // }

  ondemand::parser parser;
  ondemand::document doc;
  try {
    doc = parser.iterate(padded_input);
  } catch (simdjson_error& e) {
    return nullptr;
  }
  if (json_path.length() < 3) {
    return nullptr;
  }
  // Follow spark's format for specifying a field, e.g., "$.a.b".
  char formatted_json_path[json_path.length() + 1];
  int j = 0;
  for (int i = 0; i < json_path.length(); i++) {
    if (json_path[i] == '$' || json_path[i] == ']' || json_path[i] == '\'') {
      continue;
    } else if (json_path[i] == '[' || json_path[i] == '.') {
      formatted_json_path[j] = '/';
      j++;
    } else {
      formatted_json_path[j] = json_path[i];
      j++;
    }
  }
  formatted_json_path[j] = '\0';
  std::string res;
  error_code error;
  try {
    auto raw_res = doc.at_pointer(formatted_json_path);
    error = handle_types(raw_res, &res);
    if (error) {
      return nullptr;
    }
  } catch (...) {
    return nullptr;
  }

  *out_len = res.length();
  if (*out_len == 0) {
    return reinterpret_cast<const uint8_t*>("");
  }

  if (!is_valid(json_str, res)) {
    return nullptr;
  }

  uint8_t* result_buffer = reinterpret_cast<uint8_t*>(ctx->arena()->Allocate(*out_len));
  memcpy(result_buffer, res.data(), *out_len);
  return result_buffer;
}

}  // namespace gandiva
