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

#include "gandiva/function_registry_arithmetic.h"
#include "gandiva/function_registry_common.h"

namespace gandiva {

#define BINARY_SYMMETRIC_FN(name, ALIASES) \
  NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, name, ALIASES)

#define BINARY_RELATIONAL_BOOL_FN(name, ALIASES) \
  NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, name, ALIASES)

#define BINARY_RELATIONAL_BOOL_DATE_FN(name, ALIASES) \
  NUMERIC_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, name, ALIASES)

#define UNARY_CAST_TO_FLOAT64(name) UNARY_SAFE_NULL_IF_NULL(castFLOAT8, {}, name, float64)

#define UNARY_CAST_TO_FLOAT32(name) UNARY_SAFE_NULL_IF_NULL(castFLOAT4, {}, name, float32)

std::vector<NativeFunction> GetArithmeticFunctionRegistry() {
  static std::vector<NativeFunction> arithmetic_fn_registry_ = {
      UNARY_SAFE_NULL_IF_NULL(not, {}, boolean, boolean),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, int32, int64),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, float32, int64),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, float64, int64),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, date64, int64),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, boolean, int64),
      UNARY_SAFE_NULL_IF_NULL(castINT, {}, int8, int32),
      UNARY_SAFE_NULL_IF_NULL(castINT, {}, int16, int32),
      UNARY_SAFE_NULL_IF_NULL(castINT, {}, int64, int32),
      UNARY_SAFE_NULL_IF_NULL(castINT, {}, date32, int32),
      UNARY_SAFE_NULL_IF_NULL(castINT, {}, float32, int32),
      UNARY_SAFE_NULL_IF_NULL(castINT, {}, float64, int32),
      UNARY_SAFE_NULL_IF_NULL(castBYTE, {}, int16, int8),
      UNARY_SAFE_NULL_IF_NULL(castBYTE, {}, int32, int8),
      UNARY_SAFE_NULL_IF_NULL(castBYTE, {}, int64, int8),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, decimal128, int64),

      // cast to float32
      UNARY_CAST_TO_FLOAT32(int32), UNARY_CAST_TO_FLOAT32(int64),
      UNARY_CAST_TO_FLOAT32(float64),

      // cast to float64
      UNARY_CAST_TO_FLOAT64(int32), UNARY_CAST_TO_FLOAT64(int64),
      UNARY_CAST_TO_FLOAT64(float32), UNARY_CAST_TO_FLOAT64(decimal128),

      // cast to decimal
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {}, int32, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {}, int64, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {}, float32, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {}, float64, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {}, decimal128, decimal128),
      UNARY_UNSAFE_NULL_IF_NULL(castDECIMAL, {}, utf8, decimal128),

      // isNaN
      UNARY_SAFE_NULL_IF_NULL(isNaN, {}, float32, boolean),
      UNARY_SAFE_NULL_IF_NULL(isNaN, {}, float64, boolean),

      NativeFunction("castDECIMALNullOnOverflow", {}, DataTypeVector{decimal128()},
                     decimal128(), kResultNullInternal,
                     "castDECIMALNullOnOverflow_decimal128"),

      BINARY_SYMMETRIC_SAFE_INTERNAL_NULL(pmod, {}, int8),
      BINARY_SYMMETRIC_SAFE_INTERNAL_NULL(pmod, {}, int16),
      BINARY_SYMMETRIC_SAFE_INTERNAL_NULL(pmod, {}, int32),
      BINARY_SYMMETRIC_SAFE_INTERNAL_NULL(pmod, {}, int64),
      BINARY_SYMMETRIC_SAFE_INTERNAL_NULL(pmod, {}, float32),
      BINARY_SYMMETRIC_SAFE_INTERNAL_NULL(pmod, {}, float64),

      UNARY_SAFE_NULL_IF_NULL(castDATE, {}, int64, date64),
      UNARY_SAFE_NULL_IF_NULL(castDATE, {}, int32, date32),
      UNARY_SAFE_NULL_IF_NULL(castDATE, {}, date32, date64),

      // add/sub/multiply/divide/mod
      BINARY_SYMMETRIC_FN(add, {}), BINARY_SYMMETRIC_FN(subtract, {}),
      BINARY_SYMMETRIC_FN(multiply, {}),
      NUMERIC_TYPES(BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL, divide, {}),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, {"modulo"}, int64, int8, int8),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, {"modulo"}, int64, int16, int16),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, {"modulo"}, int64, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, {"modulo"}, int64, int64, int64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(mod, {"modulo"}, decimal128),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(mod, {"modulo"}, float32),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(mod, {"modulo"}, float64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, int32),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, int64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, float32),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, float64),

      // bitwise operators
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_and, {}, int32),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_and, {}, int64),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_or, {}, int32),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_or, {}, int64),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_xor, {}, int32),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_xor, {}, int64),
      UNARY_SAFE_NULL_IF_NULL(bitwise_not, {}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(bitwise_not, {}, int64, int64),

      // round functions
      UNARY_SAFE_NULL_IF_NULL(round, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(round, {}, float64, float64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, float32, int32, float32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, float64, int32, float64),
      UNARY_SAFE_NULL_IF_NULL(round, {}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(round, {}, int64, int64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, int32, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, int64, int32, int64),
      // normalize for nan and zero
      UNARY_SAFE_NULL_IF_NULL(normalize, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(normalize, {}, float64, float64),
      // floor
      UNARY_SAFE_NULL_IF_NULL(floor, {}, float64, int64),
      UNARY_SAFE_NULL_IF_NULL(floor, {}, int64, int64),
      // ceil
      UNARY_SAFE_NULL_IF_NULL(ceil, {}, float64, int64),
      UNARY_SAFE_NULL_IF_NULL(ceil, {}, int64, int64),
      // bitwise functions
      BINARY_GENERIC_SAFE_NULL_IF_NULL(shift_left, {}, int32, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(shift_left, {}, int64, int32, int64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(shift_right, {}, int32, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(shift_right, {}, int64, int32, int64),

      // compare functions
      BINARY_RELATIONAL_BOOL_FN(equal, ({"eq", "same"})),
      BINARY_RELATIONAL_BOOL_FN(not_equal, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(less_than, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(less_than_or_equal_to, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(greater_than, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(greater_than_or_equal_to, {}),

      // compare functions with nan
      BINARY_RELATIONAL_BOOL_FN(equal_with_nan, ({"eq_with_nan", "same_with_nan"})),
      BINARY_RELATIONAL_BOOL_FN(not_equal_with_nan, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(less_than_with_nan, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(less_than_or_equal_to_with_nan, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(greater_than_with_nan, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(greater_than_or_equal_to_with_nan, {})};

  return arithmetic_fn_registry_;
}

}  // namespace gandiva
