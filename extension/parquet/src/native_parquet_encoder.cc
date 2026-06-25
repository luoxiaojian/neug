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

#include "parquet/native_parquet_encoder.h"

#include "parquet/carquet_export_type_converter.h"
#include "parquet/carquet_export_value_converter.h"

namespace neug {
namespace reader {

neug::Status NativeParquetEncoder::writeTable(
    fsys::FileSystem& fs, const std::string& path, const QueryResponse* table,
    const std::shared_ptr<EntrySchema>& entry_schema,
    const ParquetWriteOptions& options) {
  if (!table || table->row_count() == 0) {
    return neug::Status::OK();
  }

  try {
    carquet_error_t err = CARQUET_ERROR_INIT;
    std::vector<CarquetExportColumn> columns;
    carquet_schema_t* schema = buildCarquetSchemaFromQueryResponse(
        table, entry_schema, columns, &err);
    if (schema == nullptr) {
      char buf[512];
      carquet_error_format(&err, buf, sizeof(buf));
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          std::string("Failed to build Carquet schema: ") + buf);
    }

    const carquet_writer_options_t writer_opts = buildCarquetWriterOptions(options);
    const std::string local_path = resolveCarquetExportPath(fs, path);
    carquet_writer_t* writer =
        carquet_writer_create(local_path.c_str(), schema, &writer_opts, &err);
    if (writer == nullptr) {
      carquet_schema_free(schema);
      char buf[512];
      carquet_error_format(&err, buf, sizeof(buf));
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          std::string("Failed to create Carquet writer: ") + buf);
    }

    const int64_t num_rows = table->row_count();
    for (int i = 0; i < table->arrays_size(); ++i) {
      const auto status = writeCarquetColumn(
          writer, schema, columns[static_cast<size_t>(i)], table->arrays(i),
          num_rows);
      if (!status.ok()) {
        (void)carquet_writer_close(writer);
        carquet_schema_free(schema);
        return status;
      }
    }

    if (carquet_writer_close(writer) != CARQUET_OK) {
      carquet_schema_free(schema);
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to close Carquet writer");
    }
    carquet_schema_free(schema);
    return neug::Status::OK();
  } catch (const std::exception& e) {
    return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                        std::string("Failed to write Parquet table: ") + e.what());
  }
}

}  // namespace reader
}  // namespace neug
