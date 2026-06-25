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

#include "neug/utils/io/vfs/file_system.h"
#include "neug/utils/io/write/writer.h"
#include "parquet/parquet_decoder.h"
#include "parquet/parquet_encoder.h"

namespace neug {
namespace writer {

class ParquetExportWriter : public QueryExportWriter {
 public:
  ParquetExportWriter(
      const reader::FileSchema& schema,
      std::unique_ptr<fsys::FileSystem> file_system,
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr,
      reader::ParquetBackend backend = reader::ParquetBackend::Auto);
  ~ParquetExportWriter() override = default;

  neug::Status writeTable(const QueryResponse* table) override;

 private:
  std::unique_ptr<fsys::FileSystem> file_system_;
  std::unique_ptr<reader::IParquetEncoder> encoder_;
};

}  // namespace writer
}  // namespace neug
