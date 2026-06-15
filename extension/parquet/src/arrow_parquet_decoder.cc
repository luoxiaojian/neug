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

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <glog/logging.h>
#include <parquet/arrow/reader.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/data_chunk.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"
#include "parquet/arrow_context_column.h"
#include "parquet/arrow_parquet_decoder.h"
#include "parquet/arrow_random_access_file.h"
#include "parquet/arrow_type_converter.h"
#include "parquet/record_batch_supplier.h"

namespace neug {
namespace reader {
namespace {

std::vector<std::string> resolveReadColumnNames(
    const ReadSharedState& state,
    const std::shared_ptr<arrow::Schema>& file_schema) {
  if (!state.schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }
  const auto& entry_names = state.schema.entry->columnNames;
  for (const auto& name : entry_names) {
    if (file_schema->GetFieldIndex(name) < 0) {
      THROW_SCHEMA_MISMATCH("Column '" + name +
                            "' not found in file. Available columns: " +
                            file_schema->ToString());
    }
  }
  return entry_names;
}

std::vector<int> resolveReadColumnIndices(
    const std::shared_ptr<arrow::Schema>& file_schema,
    const std::vector<std::string>& read_names) {
  std::vector<int> indices;
  indices.reserve(read_names.size());
  for (const auto& name : read_names) {
    indices.push_back(file_schema->GetFieldIndex(name));
  }
  return indices;
}

std::unique_ptr<parquet::arrow::FileReader> openParquetFileReader(
    fsys::FileSystem& fs, const std::string& path,
    const ParquetReadOptions& options) {
  auto input = fs.openInputFile(path);
  if (!input) {
    THROW_IO_EXCEPTION("Failed to open Parquet file: " + path);
  }
  auto arrow_file = parquet_adapt::wrapRandomAccessFile(std::move(input));

  parquet::arrow::FileReaderBuilder builder;
  auto status = builder.Open(arrow_file, *options.reader_properties);
  if (!status.ok()) {
    THROW_IO_EXCEPTION("Failed to open Parquet reader for " + path + ": " +
                       status.ToString());
  }
  builder.memory_pool(arrow::default_memory_pool());
  builder.properties(*options.arrow_reader_properties);

  std::unique_ptr<parquet::arrow::FileReader> reader;
  status = builder.Build(&reader);
  if (!status.ok()) {
    THROW_IO_EXCEPTION("Failed to build Parquet reader for " + path + ": " +
                       status.ToString());
  }
  return reader;
}

execution::DataChunk tableToValueDataChunk(
    const std::shared_ptr<arrow::Table>& table) {
  if (!table) {
    THROW_IO_EXCEPTION("Parquet table is null");
  }
  execution::DataChunk chunk;
  for (int i = 0; i < table->num_columns(); ++i) {
    auto table_column = table->column(i);
    execution::ArrowArrayContextColumnBuilder builder;
    for (const auto& array : table_column->chunks()) {
      builder.push_back(array);
    }
    auto finished = builder.finish();
    auto arrow_col =
        std::dynamic_pointer_cast<execution::ArrowArrayContextColumn>(finished);
    if (!arrow_col) {
      THROW_IO_EXCEPTION("Failed to convert Parquet column to Arrow context");
    }
    chunk.set(i, arrow_col->cast_to_value_column());
  }
  return chunk;
}

result<std::shared_ptr<EntrySchema>> convertArrowSchemaToEntrySchema(
    const std::shared_ptr<arrow::Schema>& arrow_schema) {
  if (!arrow_schema) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Arrow schema is null");
  }

  auto entry_schema = std::make_shared<TableEntrySchema>();
  ArrowTypeConverter converter;
  const int num_fields = arrow_schema->num_fields();

  entry_schema->columnNames.reserve(num_fields);
  entry_schema->columnTypes.reserve(num_fields);

  for (int i = 0; i < num_fields; ++i) {
    const auto& field = arrow_schema->field(i);
    const std::string& column_name = field->name();

    entry_schema->columnNames.push_back(column_name);

    auto common_type = converter.convert(*field->type());
    if (!common_type) {
      RETURN_STATUS_ERROR(
          neug::StatusCode::ERR_TYPE_CONVERSION,
          "Failed to convert Arrow type for column: " + column_name);
    }
    entry_schema->columnTypes.push_back(std::move(common_type));
  }

  return entry_schema;
}

class ParquetFileChunkSupplier : public IDataChunkSupplier {
 public:
  ParquetFileChunkSupplier(fsys::FileSystem* fs, std::string path,
                           ParquetReadOptions options,
                           std::vector<int> column_indices, bool batch_read)
      : fs_(fs),
        path_(std::move(path)),
        options_(std::move(options)),
        column_indices_(std::move(column_indices)),
        batch_read_(batch_read) {}

  std::shared_ptr<execution::DataChunk> GetNextChunk() override {
    if (finished_) {
      return nullptr;
    }
    if (!reader_) {
      reader_ = openParquetFileReader(*fs_, path_, options_);
      if (row_num_ == 0) {
        row_num_ = reader_->parquet_reader()->metadata()->num_rows();
      }
      if (batch_read_) {
        std::shared_ptr<arrow::RecordBatchReader> batch_reader;
        auto status =
            reader_->GetRecordBatchReader(column_indices_, &batch_reader);
        if (!status.ok()) {
          THROW_IO_EXCEPTION("Failed to create Parquet batch reader for " +
                             path_ + ": " + status.ToString());
        }
        batch_reader_ = std::move(batch_reader);
      } else {
        std::shared_ptr<arrow::Table> table;
        auto status = reader_->ReadTable(column_indices_, &table);
        if (!status.ok()) {
          THROW_IO_EXCEPTION("Failed to read Parquet table from " + path_ +
                             ": " + status.ToString());
        }
        cached_chunk_ =
            std::make_shared<execution::DataChunk>(tableToValueDataChunk(table));
        finished_ = true;
        return cached_chunk_;
      }
    }

    if (!batch_read_) {
      finished_ = true;
      return cached_chunk_;
    }

    auto result = batch_reader_->Next();
    if (!result.ok()) {
      THROW_IO_EXCEPTION("Failed to read Parquet batch from " + path_ + ": " +
                         result.status().ToString());
    }
    auto batch = result.ValueOrDie();
    if (!batch) {
      finished_ = true;
      return nullptr;
    }
    return execution::recordbatch_to_value_datachunk(batch);
  }

  int64_t RowNum() const override { return row_num_; }

 private:
  fsys::FileSystem* fs_;
  std::string path_;
  ParquetReadOptions options_;
  std::vector<int> column_indices_;
  bool batch_read_ = true;
  int64_t row_num_ = 0;
  bool finished_ = false;
  std::unique_ptr<parquet::arrow::FileReader> reader_;
  std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
  std::shared_ptr<execution::DataChunk> cached_chunk_;
};

}  // namespace

result<std::shared_ptr<EntrySchema>> ArrowParquetDecoder::inferSchema(
    fsys::FileSystem& fs, const std::vector<std::string>& paths,
    const ParquetReadOptions& options) {
  if (paths.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "No file paths provided");
  }

  auto reader = openParquetFileReader(fs, paths.front(), options);
  std::shared_ptr<arrow::Schema> arrow_schema;
  auto status = reader->GetSchema(&arrow_schema);
  if (!status.ok()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                        "Failed to infer schema from Parquet file: " +
                            status.ToString());
  }
  return convertArrowSchemaToEntrySchema(arrow_schema);
}

std::vector<std::shared_ptr<IDataChunkSupplier>>
ArrowParquetDecoder::openSuppliers(fsys::FileSystem& fs,
                                   const ReadSharedState& state,
                                   const ParquetReadOptions& options,
                                   bool batch_read) {
  const auto& file_paths = state.schema.file.paths;
  if (file_paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  auto first_reader = openParquetFileReader(fs, file_paths.front(), options);
  std::shared_ptr<arrow::Schema> file_schema;
  auto schema_status = first_reader->GetSchema(&file_schema);
  if (!schema_status.ok()) {
    THROW_IO_EXCEPTION("Failed to read Parquet schema: " +
                       schema_status.ToString());
  }

  const auto read_column_names = resolveReadColumnNames(state, file_schema);
  const auto column_indices =
      resolveReadColumnIndices(file_schema, read_column_names);

  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers;
  suppliers.reserve(file_paths.size());
  for (const auto& path : file_paths) {
    suppliers.push_back(std::make_shared<ParquetFileChunkSupplier>(
        &fs, path, options, column_indices, batch_read));
  }
  return suppliers;
}

}  // namespace reader
}  // namespace neug
