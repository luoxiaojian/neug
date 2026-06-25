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

#include "parquet/parquet_decoder.h"

#include <algorithm>
#include <cctype>
#include <memory>

#include "neug/utils/exception/exception.h"

#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
#include "parquet/arrow_parquet_decoder.h"
#else
#include "parquet/native_parquet_decoder.h"
#endif

namespace neug {
namespace reader {

namespace {

static Option<std::string> parquetBackendOption() {
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  return Option<std::string>::StringOption("PARQUET_BACKEND", "arrow");
#else
  return Option<std::string>::StringOption("PARQUET_BACKEND", "native");
#endif
}

std::string toLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

}  // namespace

ParquetBackend parseParquetBackend(const options_t& options) {
  const std::string backend = toLower(parquetBackendOption().get(options));
  if (backend == "arrow") {
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
    return ParquetBackend::Arrow;
#else
    return ParquetBackend::Native;
#endif
  }
  if (backend == "auto") {
    return ParquetBackend::Auto;
  }
  return ParquetBackend::Native;
}

std::unique_ptr<IParquetDecoder> createParquetDecoder(ParquetBackend backend) {
  (void)backend;  // Backend selection is compile-time only.
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  return std::make_unique<ArrowParquetDecoder>();
#else
  return std::make_unique<NativeParquetDecoder>();
#endif
}

std::vector<std::string> entryColumnNames(const ReadSharedState& state) {
  if (!state.schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }
  return state.schema.entry->columnNames;
}

}  // namespace reader
}  // namespace neug
