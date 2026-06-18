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

#include "parquet/parquet_export_writer.h"

namespace neug {
namespace writer {

ParquetExportWriter::ParquetExportWriter(
    const reader::FileSchema& schema,
    std::unique_ptr<fsys::FileSystem> file_system,
    std::shared_ptr<reader::EntrySchema> entry_schema,
    reader::ParquetBackend backend)
    : QueryExportWriter(schema, std::move(entry_schema)),
      file_system_(std::move(file_system)),
      encoder_(reader::createParquetEncoder(backend)) {}

neug::Status ParquetExportWriter::writeTable(const QueryResponse* table) {
  if (schema_.paths.empty()) {
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Schema paths is empty");
  }
  reader::ParquetExportOptionsBuilder options_builder(schema_.options);
  const auto write_options = options_builder.build();
  return encoder_->writeTable(*file_system_, schema_.paths[0], table,
                              entry_schema_, write_options);
}

}  // namespace writer
}  // namespace neug
