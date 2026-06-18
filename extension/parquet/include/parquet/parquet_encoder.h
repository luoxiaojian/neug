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

#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/vfs/file_system.h"
#include "neug/utils/result.h"
#include "parquet/parquet_decoder.h"
#include "parquet/parquet_options.h"

namespace neug {
namespace reader {

/// Encodes QueryResponse tables into Parquet bytes via VFS.
class IParquetEncoder {
 public:
  virtual ~IParquetEncoder() = default;

  virtual neug::Status writeTable(
      fsys::FileSystem& fs, const std::string& path,
      const QueryResponse* table,
      const std::shared_ptr<EntrySchema>& entry_schema,
      const ParquetWriteOptions& options) = 0;
};

std::unique_ptr<IParquetEncoder> createParquetEncoder(ParquetBackend backend);

}  // namespace reader
}  // namespace neug
