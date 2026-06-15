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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/data_chunk.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/row_expression_filter.h"
#include "neug/utils/result.h"
#include "parquet/arrow_context_column.h"
#include "parquet/arrow_random_access_file.h"
#include "parquet/arrow_reader.h"
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

class TransformingChunkSupplier : public IDataChunkSupplier {
 public:
  TransformingChunkSupplier(
      std::shared_ptr<IDataChunkSupplier> inner,
      std::shared_ptr<::common::Expression> filter_expr,
      std::vector<std::string> column_names,
      std::vector<std::string> project_columns)
      : inner_(std::move(inner)),
        filter_expr_(std::move(filter_expr)),
        column_names_(std::move(column_names)),
        project_columns_(std::move(project_columns)) {}

  std::shared_ptr<execution::DataChunk> GetNextChunk() override {
    while (true) {
      auto chunk = inner_->GetNextChunk();
      if (!chunk) {
        return nullptr;
      }
      auto filtered =
          filter_chunk(*chunk, filter_expr_, column_names_);
      auto projected = project_chunk(filtered, column_names_, project_columns_);
      if (projected.row_num() == 0 && filter_expr_) {
        continue;
      }
      return std::make_shared<execution::DataChunk>(std::move(projected));
    }
  }

  int64_t RowNum() const override { return inner_->RowNum(); }

 private:
  std::shared_ptr<IDataChunkSupplier> inner_;
  std::shared_ptr<::common::Expression> filter_expr_;
  std::vector<std::string> column_names_;
  std::vector<std::string> project_columns_;
};

std::shared_ptr<IDataChunkSupplier> maybeWrapTransform(
    std::shared_ptr<IDataChunkSupplier> supplier,
    const ReadSharedState& state,
    const std::vector<std::string>& file_column_names) {
  if (!state.skipRows && state.projectColumns.empty()) {
    return supplier;
  }
  return std::make_shared<TransformingChunkSupplier>(
      std::move(supplier), state.skipRows, file_column_names,
      state.projectColumns);
}

}  // namespace

std::shared_ptr<IDataChunkSupplier> ArrowReader::read() {
  if (!sharedState_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  if (!fileSystem_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FileSystem is null");
  }
  if (!optionsBuilder_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Options builder is null");
  }

  const auto& fileSchema = sharedState_->schema.file;
  const auto& file_paths = fileSchema.paths;
  if (file_paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  auto options = optionsBuilder_->build();
  ReadOptions read_options;
  const bool use_batch_read = read_options.batch_read.get(fileSchema.options);

  auto first_reader = openParquetFileReader(*fileSystem_, file_paths.front(),
                                            options);
  std::shared_ptr<arrow::Schema> file_schema;
  auto schema_status = first_reader->GetSchema(&file_schema);
  if (!schema_status.ok()) {
    THROW_IO_EXCEPTION("Failed to read Parquet schema: " +
                       schema_status.ToString());
  }

  const auto read_column_names = resolveReadColumnNames(*sharedState_, file_schema);
  const auto column_indices =
      resolveReadColumnIndices(file_schema, read_column_names);

  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers;
  suppliers.reserve(file_paths.size());
  for (const auto& path : file_paths) {
    suppliers.push_back(std::make_shared<ParquetFileChunkSupplier>(
        fileSystem_.get(), path, options, column_indices, use_batch_read));
  }

  if (use_batch_read) {
    auto supplier = batch_read(suppliers);
    return maybeWrapTransform(std::move(supplier), *sharedState_,
                              read_column_names);
  }
  return full_read(suppliers, read_column_names);
}

std::shared_ptr<IDataChunkSupplier> ArrowReader::full_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    const std::vector<std::string>& file_column_names) {
  auto merged = read_all_chunks(suppliers);

  int expected_cols = sharedState_->columnNum();
  if (expected_cols > 0 &&
      static_cast<int>(merged.col_num()) != expected_cols &&
      sharedState_->projectColumns.empty()) {
    THROW_IO_EXCEPTION(
        "Column number mismatch between schema and Parquet data, schema: " +
        std::to_string(expected_cols) + ", data: " +
        std::to_string(merged.col_num()));
  }

  auto filtered =
      filter_chunk(merged, sharedState_->skipRows, file_column_names);
  auto projected = project_chunk(filtered, file_column_names,
                                 sharedState_->projectColumns.empty()
                                     ? file_column_names
                                     : sharedState_->projectColumns);
  return std::make_shared<ChunkSupplierWrapper>(
      std::vector<std::shared_ptr<IDataChunkSupplier>>{},
      std::make_shared<execution::DataChunk>(std::move(projected)));
}

std::shared_ptr<IDataChunkSupplier> ArrowReader::batch_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers) {
  if (suppliers.size() == 1) {
    return suppliers.front();
  }
  return std::make_shared<ChunkSupplierWrapper>(suppliers);
}

result<std::shared_ptr<EntrySchema>> ArrowReader::inferSchema() {
  if (!sharedState_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "SharedState is null");
  }
  if (!fileSystem_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "FileSystem is null");
  }
  if (!optionsBuilder_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Options builder is null");
  }

  const auto& file_paths = sharedState_->schema.file.paths;
  if (file_paths.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "No file paths provided");
  }

  auto options = optionsBuilder_->build();
  auto reader = openParquetFileReader(*fileSystem_, file_paths.front(), options);
  std::shared_ptr<arrow::Schema> arrow_schema;
  auto status = reader->GetSchema(&arrow_schema);
  if (!status.ok()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                        "Failed to infer schema from Parquet file: " +
                            status.ToString());
  }
  return convertArrowSchemaToEntrySchema(arrow_schema);
}

result<std::shared_ptr<EntrySchema>> ArrowReader::convertArrowSchemaToEntrySchema(
    const std::shared_ptr<arrow::Schema>& arrowSchema) {
  if (!arrowSchema) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Arrow schema is null");
  }

  auto entrySchema = std::make_shared<TableEntrySchema>();
  ArrowTypeConverter converter;
  int numFields = arrowSchema->num_fields();

  entrySchema->columnNames.reserve(numFields);
  entrySchema->columnTypes.reserve(numFields);

  for (int i = 0; i < numFields; ++i) {
    const auto& field = arrowSchema->field(i);
    const std::string& columnName = field->name();

    entrySchema->columnNames.push_back(columnName);

    auto commonType = converter.convert(*field->type());
    if (!commonType) {
      RETURN_STATUS_ERROR(
          neug::StatusCode::ERR_TYPE_CONVERSION,
          "Failed to convert Arrow type for column: " + columnName);
    }
    entrySchema->columnTypes.push_back(std::move(commonType));
  }

  return entrySchema;
}

}  // namespace reader
}  // namespace neug
