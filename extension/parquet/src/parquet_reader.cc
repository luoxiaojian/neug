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

#include "parquet/parquet_reader.h"

#include <memory>
#include <utility>
#include <vector>

#include "neug/execution/common/data_chunk.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/row_expression_filter.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {
namespace {

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

std::shared_ptr<IDataChunkSupplier> mergeSuppliers(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    bool batch_read) {
  if (suppliers.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No Parquet suppliers to read");
  }
  if (batch_read && suppliers.size() == 1) {
    return suppliers.front();
  }
  if (batch_read) {
    return std::make_shared<ChunkSupplierWrapper>(suppliers);
  }
  auto merged = read_all_chunks(suppliers);
  return std::make_shared<ChunkSupplierWrapper>(
      std::vector<std::shared_ptr<IDataChunkSupplier>>{},
      std::make_shared<execution::DataChunk>(std::move(merged)));
}

std::vector<std::string> entryColumnNames(const ReadSharedState& state) {
  if (!state.schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }
  return state.schema.entry->columnNames;
}

}  // namespace

ParquetReader::ParquetReader(std::shared_ptr<ReadSharedState> sharedState,
                               std::unique_ptr<ParquetOptionsBuilder> optionsBuilder,
                               std::unique_ptr<fsys::FileSystem> fileSystem,
                               ParquetBackend backend)
    : sharedState_(std::move(sharedState)),
      fileSystem_(std::move(fileSystem)),
      optionsBuilder_(std::move(optionsBuilder)),
      backend_(backend) {}

std::shared_ptr<IDataChunkSupplier> ParquetReader::read() {
  if (!sharedState_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  if (!fileSystem_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FileSystem is null");
  }
  if (!optionsBuilder_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Options builder is null");
  }

  const auto& file_schema = sharedState_->schema.file;
  if (file_schema.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  ParquetBackend backend = backend_;
  if (backend == ParquetBackend::Auto) {
    backend = parseParquetBackend(file_schema.options);
    if (backend == ParquetBackend::Auto) {
      backend = ParquetBackend::Arrow;
    }
  }

  auto options = optionsBuilder_->build();
  ReadOptions read_options;
  const bool use_batch_read = read_options.batch_read.get(file_schema.options);

  auto decoder = createParquetDecoder(backend);
  auto suppliers = decoder->openSuppliers(*fileSystem_, *sharedState_, options,
                                          use_batch_read);
  const auto file_column_names = entryColumnNames(*sharedState_);
  return finalizeSuppliers(std::move(suppliers), use_batch_read,
                           file_column_names);
}

std::shared_ptr<IDataChunkSupplier> ParquetReader::finalizeSuppliers(
    std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers,
    bool batch_read, const std::vector<std::string>& file_column_names) {
  if (!batch_read) {
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

  auto supplier = mergeSuppliers(suppliers, true);
  return maybeWrapTransform(std::move(supplier), *sharedState_,
                            file_column_names);
}

result<std::shared_ptr<EntrySchema>> ParquetReader::inferSchema() {
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

  ParquetBackend backend = backend_;
  if (backend == ParquetBackend::Auto) {
    backend = parseParquetBackend(sharedState_->schema.file.options);
    if (backend == ParquetBackend::Auto) {
      backend = ParquetBackend::Arrow;
    }
  }

  auto options = optionsBuilder_->build();
  return createParquetDecoder(backend)->inferSchema(*fileSystem_, file_paths,
                                                    options);
}

std::unique_ptr<ParquetReader> createParquetReader(
    std::shared_ptr<ReadSharedState> sharedState,
    std::unique_ptr<fsys::FileSystem> fileSystem) {
  auto options_builder = std::make_unique<ParquetOptionsBuilder>(sharedState);
  return std::make_unique<ParquetReader>(
      std::move(sharedState), std::move(options_builder),
      std::move(fileSystem), ParquetBackend::Auto);
}

}  // namespace reader
}  // namespace neug
