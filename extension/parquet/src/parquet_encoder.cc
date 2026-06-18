/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "parquet/parquet_encoder.h"

#include <memory>

#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
#include "parquet/arrow_parquet_encoder.h"
#else
#include "parquet/native_parquet_encoder.h"
#endif

namespace neug {
namespace reader {

std::unique_ptr<IParquetEncoder> createParquetEncoder(ParquetBackend backend) {
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  switch (backend) {
  case ParquetBackend::Arrow:
  case ParquetBackend::Auto:
  case ParquetBackend::Native:
  default:
    return std::make_unique<ArrowParquetEncoder>();
  }
#else
  switch (backend) {
  case ParquetBackend::Native:
  case ParquetBackend::Auto:
  case ParquetBackend::Arrow:
  default:
    return std::make_unique<NativeParquetEncoder>();
  }
#endif
}

}  // namespace reader
}  // namespace neug
