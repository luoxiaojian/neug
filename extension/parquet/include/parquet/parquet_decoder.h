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
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/read_state.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/vfs/file_system.h"
#include "neug/utils/result.h"
#include "parquet_options.h"

namespace neug {
namespace reader {

enum class ParquetBackend { Arrow, Native, Auto };

/// Parses PARQUET_BACKEND from file options (arrow | native | auto).
ParquetBackend parseParquetBackend(const options_t& options);

/// Decodes Parquet bytes from VFS into IDataChunkSupplier streams.
class IParquetDecoder {
 public:
  virtual ~IParquetDecoder() = default;

  virtual result<std::shared_ptr<EntrySchema>> inferSchema(
      fsys::FileSystem& fs, const std::vector<std::string>& paths,
      const ParquetReadOptions& options) = 0;

  /// Returns one supplier per input path; columns match entry schema order.
  virtual std::vector<std::shared_ptr<IDataChunkSupplier>> openSuppliers(
      fsys::FileSystem& fs, const ReadSharedState& state,
      const ParquetReadOptions& options, bool batch_read) = 0;
};

std::unique_ptr<IParquetDecoder> createParquetDecoder(ParquetBackend backend);

/// Returns the column names from the entry schema in ReadSharedState.
/// Throws if the entry schema is null.
std::vector<std::string> entryColumnNames(const ReadSharedState& state);

}  // namespace reader
}  // namespace neug
