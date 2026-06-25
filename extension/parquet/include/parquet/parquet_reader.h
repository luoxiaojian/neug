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

#include "neug/utils/io/read/common/file_reader.h"
#include "neug/utils/io/vfs/file_system.h"
#include "parquet/parquet_decoder.h"
#include "parquet_options.h"

namespace neug {
namespace reader {

/// Format-level Parquet reader; delegates decode to IParquetDecoder backends.
class ParquetReader : public FileReader {
 public:
  ParquetReader(std::shared_ptr<ReadSharedState> sharedState,
                std::unique_ptr<ParquetOptionsBuilder> optionsBuilder,
                std::unique_ptr<fsys::FileSystem> fileSystem,
                ParquetBackend backend = ParquetBackend::Auto);
  ~ParquetReader() override = default;

  std::shared_ptr<IDataChunkSupplier> read() override;

  result<std::shared_ptr<EntrySchema>> inferSchema() override;

 private:
  std::shared_ptr<IDataChunkSupplier> finalizeSuppliers(
      std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers,
      bool batch_read, const std::vector<std::string>& file_column_names);

  std::shared_ptr<ReadSharedState> sharedState_;
  std::unique_ptr<fsys::FileSystem> fileSystem_;
  std::unique_ptr<ParquetOptionsBuilder> optionsBuilder_;
  ParquetBackend backend_;
};

std::unique_ptr<ParquetReader> createParquetReader(
    std::shared_ptr<ReadSharedState> sharedState,
    std::unique_ptr<fsys::FileSystem> fileSystem);

// Backward-compatible alias; prefer ParquetReader for new code.
using ArrowReader = ParquetReader;

}  // namespace reader
}  // namespace neug
