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

#include "parquet/native_parquet_decoder.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/data_chunk.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"
#include "parquet/carquet_type_converter.h"
#include "parquet/carquet_value_converter.h"

namespace neug {
namespace reader {
namespace {

std::vector<std::string> entryColumnNames(const ReadSharedState& state) {
  if (!state.schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }
  return state.schema.entry->columnNames;
}

std::vector<carquet_physical_type_t> projectedPhysicalTypes(
    const carquet_schema_t* schema, const std::vector<int32_t>& indices) {
  std::vector<carquet_physical_type_t> types;
  types.reserve(indices.size());
  for (const int32_t idx : indices) {
    types.push_back(carquet_schema_column_type(schema, idx));
  }
  return types;
}

std::vector<const carquet_logical_type_t*> projectedLogicalTypes(
    const carquet_schema_t* schema, const std::vector<int32_t>& indices) {
  std::vector<const carquet_logical_type_t*> types;
  types.reserve(indices.size());
  for (const int32_t idx : indices) {
    types.push_back(carquetLeafLogicalType(schema, idx));
  }
  return types;
}

carquet_batch_reader_t* createBatchReader(
    carquet_reader_t* reader, const ParquetReadOptions& options,
    const std::vector<std::string>& column_names,
    carquet_thread_pool_t** owned_pool) {
  carquet_batch_reader_config_t config;
  carquet_batch_reader_config_init(&config);
  config.batch_size = static_cast<int32_t>(options.batch_size);
  config.use_mmap = options.use_mmap;
  config.num_threads = options.use_threads ? 0 : 1;

  std::vector<const char*> name_ptrs;
  name_ptrs.reserve(column_names.size());
  for (const auto& name : column_names) {
    name_ptrs.push_back(name.c_str());
  }
  if (!name_ptrs.empty()) {
    config.column_names = name_ptrs.data();
    config.num_column_names = static_cast<int32_t>(name_ptrs.size());
  }

  if (options.use_threads) {
    *owned_pool = carquet_thread_pool_create(0);
    config.thread_pool = *owned_pool;
  }

  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_batch_reader_t* batch_reader =
      carquet_batch_reader_create(reader, &config, &err);
  if (batch_reader == nullptr) {
    char buf[512];
    carquet_error_format(&err, buf, sizeof(buf));
    THROW_IO_EXCEPTION("Failed to create Carquet batch reader: " +
                       std::string(buf));
  }
  return batch_reader;
}

class CarquetFileChunkSupplier : public IDataChunkSupplier {
 public:
  CarquetFileChunkSupplier(fsys::FileSystem* fs, std::string path,
                           ParquetReadOptions options,
                           std::vector<std::string> read_column_names,
                           bool batch_read)
      : fs_(fs),
        path_(std::move(path)),
        options_(std::move(options)),
        read_column_names_(std::move(read_column_names)),
        batch_read_(batch_read) {}

  ~CarquetFileChunkSupplier() override { reset(); }

  std::shared_ptr<execution::DataChunk> GetNextChunk() override {
    if (finished_) {
      return nullptr;
    }
    ensureOpen();
    if (has_nested_) {
      auto chunk = std::make_shared<execution::DataChunk>(
          readCarquetProjectedColumns(reader_handle_.reader, projected_));
      finished_ = true;
      return chunk;
    }
    if (!batch_read_) {
      auto chunk = readAllRows();
      finished_ = true;
      return chunk;
    }

    carquet_row_batch_t* batch = nullptr;
    const carquet_status_t status =
        carquet_batch_reader_next(batch_reader_, &batch);
    if (status == CARQUET_ERROR_END_OF_DATA || batch == nullptr) {
      finished_ = true;
      return nullptr;
    }
    if (status != CARQUET_OK) {
      THROW_IO_EXCEPTION("Failed to read Carquet batch from " + path_);
    }

    auto chunk = std::make_shared<execution::DataChunk>(carquetBatchToDataChunk(
        batch, physical_types_, logical_types_));
    carquet_row_batch_free(batch);
    return chunk;
  }

  int64_t RowNum() const override { return row_num_; }

 private:
  void reset() {
    if (batch_reader_ != nullptr) {
      carquet_batch_reader_free(batch_reader_);
      batch_reader_ = nullptr;
    }
    if (thread_pool_ != nullptr) {
      carquet_thread_pool_destroy(thread_pool_);
      thread_pool_ = nullptr;
    }
    closeCarquetReader(reader_handle_);
  }

  void ensureOpen() {
    if (batch_reader_ != nullptr || reader_handle_.reader != nullptr) {
      return;
    }
    reader_handle_ = openCarquetReader(*fs_, path_, options_);
    row_num_ = carquet_reader_num_rows(reader_handle_.reader);
    const auto* schema = carquet_reader_schema(reader_handle_.reader);
    projected_ = resolveCarquetProjectedColumns(schema, read_column_names_);
    has_nested_ = std::any_of(
        projected_.begin(), projected_.end(), [](const CarquetProjectedColumn& col) {
          return col.layout != CarquetColumnLayout::kFlat;
        });
    column_indices_.clear();
    column_indices_.reserve(projected_.size());
    for (const auto& column : projected_) {
      column_indices_.push_back(column.leaf_index);
    }
    physical_types_ = projectedPhysicalTypes(schema, column_indices_);
    logical_types_ = projectedLogicalTypes(schema, column_indices_);
    if (!has_nested_) {
      batch_reader_ = createBatchReader(reader_handle_.reader, options_,
                                        read_column_names_, &thread_pool_);
    }
  }

  std::shared_ptr<execution::DataChunk> readAllRows() {
    std::vector<std::shared_ptr<execution::IContextColumn>> columns;
    carquet_row_batch_t* batch = nullptr;
    while (carquet_batch_reader_next(batch_reader_, &batch) == CARQUET_OK &&
           batch != nullptr) {
      auto chunk = carquetBatchToDataChunk(batch, physical_types_,
                                          logical_types_);
      if (columns.empty()) {
        columns = chunk.columns;
      } else {
        for (size_t i = 0; i < columns.size(); ++i) {
          columns[i] = columns[i]->union_col(chunk.columns[i]);
        }
      }
      carquet_row_batch_free(batch);
      batch = nullptr;
    }

    execution::DataChunk merged;
    for (size_t i = 0; i < columns.size(); ++i) {
      merged.set(static_cast<int>(i), columns[i]);
    }
    return std::make_shared<execution::DataChunk>(std::move(merged));
  }

  fsys::FileSystem* fs_;
  std::string path_;
  ParquetReadOptions options_;
  std::vector<std::string> read_column_names_;
  bool batch_read_ = true;
  int64_t row_num_ = 0;
  bool finished_ = false;

  CarquetReaderHandle reader_handle_;
  carquet_batch_reader_t* batch_reader_ = nullptr;
  carquet_thread_pool_t* thread_pool_ = nullptr;
  std::vector<int32_t> column_indices_;
  std::vector<CarquetProjectedColumn> projected_;
  bool has_nested_ = false;
  std::vector<carquet_physical_type_t> physical_types_;
  std::vector<const carquet_logical_type_t*> logical_types_;
};

}  // namespace

result<std::shared_ptr<EntrySchema>> NativeParquetDecoder::inferSchema(
    fsys::FileSystem& fs, const std::vector<std::string>& paths,
    const ParquetReadOptions& options) {
  if (paths.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "No file paths provided");
  }

  auto handle = openCarquetReader(fs, paths.front(), options);
  const auto* schema = carquet_reader_schema(handle.reader);
  auto converted = carquetSchemaToEntrySchema(schema);
  closeCarquetReader(handle);
  return converted;
}

std::vector<std::shared_ptr<IDataChunkSupplier>>
NativeParquetDecoder::openSuppliers(fsys::FileSystem& fs,
                                    const ReadSharedState& state,
                                    const ParquetReadOptions& options,
                                    bool batch_read) {
  const auto& file_paths = state.schema.file.paths;
  if (file_paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  auto probe = openCarquetReader(fs, file_paths.front(), options);
  const auto* schema = carquet_reader_schema(probe.reader);
  const auto read_column_names = entryColumnNames(state);
  for (const auto& name : read_column_names) {
    if (carquetSchemaFindTopLevelColumn(schema, name.c_str()) < 0) {
      closeCarquetReader(probe);
      THROW_SCHEMA_MISMATCH("Column '" + name + "' not found in Parquet file");
    }
  }
  closeCarquetReader(probe);

  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers;
  suppliers.reserve(file_paths.size());
  for (const auto& path : file_paths) {
    suppliers.push_back(std::make_shared<CarquetFileChunkSupplier>(
        &fs, path, options, read_column_names, batch_read));
  }
  return suppliers;
}

}  // namespace reader
}  // namespace neug
