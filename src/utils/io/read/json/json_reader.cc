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

#include "neug/utils/io/read/json/json_reader.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "neug/execution/common/columns/columns_utils.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/row_expression_filter.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/read/common/type_converter.h"
#include "neug/utils/result.h"
#include "neug/utils/service_utils.h"

namespace neug {
namespace reader {
namespace {

constexpr size_t kDefaultJsonChunkRows = 4096;
constexpr size_t kMaxJsonChunkRows = 65536;

size_t resolve_chunk_size(const JsonReadConfig& config) {
  if (config.chunk_size <= 0) {
    return kDefaultJsonChunkRows;
  }
  return static_cast<size_t>(
      std::clamp<int64_t>(config.chunk_size, 1, kMaxJsonChunkRows));
}

std::string read_file_to_string(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    THROW_IO_EXCEPTION("Failed to open file: " + path);
  }
  std::ostringstream ss;
  ss << input.rdbuf();
  return ss.str();
}

std::vector<std::string> convert_json_array_to_lines(const std::string& path) {
  const auto content = read_file_to_string(path);
  rapidjson::Document document;
  document.Parse(content.c_str(), content.size());
  if (document.HasParseError()) {
    THROW_IO_EXCEPTION("JSON parse error in file " + path + " at offset " +
                       std::to_string(document.GetErrorOffset()) + ": " +
                       rapidjson::GetParseError_En(document.GetParseError()));
  }
  if (!document.IsArray() || document.Empty()) {
    THROW_IO_EXCEPTION("Expected non-empty JSON array in file: " + path);
  }
  std::vector<std::string> lines;
  lines.reserve(document.Size());
  for (const auto& obj : document.GetArray()) {
    if (!obj.IsObject()) {
      THROW_IO_EXCEPTION("Expected JSON object in array in file: " + path);
    }
    lines.push_back(rapidjson_stringify(obj));
  }
  return lines;
}

execution::Value parse_json_value(const rapidjson::Value& value,
                                  const DataType& data_type) {
  if (value.IsNull()) {
    return execution::Value(data_type);
  }
  switch (data_type.id()) {
  case DataTypeId::kBoolean:
    return execution::Value::BOOLEAN(value.GetBool());
  case DataTypeId::kInt32:
    if (value.IsInt()) {
      return execution::Value::INT32(value.GetInt());
    }
    return execution::Value::INT32(static_cast<int32_t>(value.GetInt64()));
  case DataTypeId::kUInt32:
    return execution::Value::UINT32(value.GetUint());
  case DataTypeId::kInt64:
    return execution::Value::INT64(value.GetInt64());
  case DataTypeId::kUInt64:
    return execution::Value::UINT64(value.GetUint64());
  case DataTypeId::kFloat:
    return execution::Value::FLOAT(static_cast<float>(value.GetDouble()));
  case DataTypeId::kDouble:
    return execution::Value::DOUBLE(value.GetDouble());
  case DataTypeId::kVarchar:
    if (value.IsString()) {
      return execution::Value::STRING(value.GetString());
    }
    return execution::Value::STRING(rapidjson_stringify(value));
  default:
    if (value.IsString()) {
      return execution::Value::STRING(value.GetString());
    }
    return execution::Value::STRING(rapidjson_stringify(value));
  }
}

class JsonChunkSupplier : public IDataChunkSupplier {
 public:
  JsonChunkSupplier(const std::string& file_path, JsonReadConfig config)
      : file_path_(file_path), config_(std::move(config)) {
    if (config_.json_array_input) {
      lines_ = convert_json_array_to_lines(file_path_);
      line_index_ = 0;
      row_num_ = static_cast<int64_t>(lines_.size());
    } else {
      input_ = std::make_unique<std::ifstream>(file_path_);
      if (!input_->is_open()) {
        THROW_IO_EXCEPTION("Failed to open JSON file: " + file_path_);
      }
      std::string line;
      while (std::getline(*input_, line)) {
        if (!line.empty()) {
          ++row_num_;
        }
      }
      input_ = std::make_unique<std::ifstream>(file_path_);
    }
    chunk_size_ = resolve_chunk_size(config_);
  }

  std::shared_ptr<execution::DataChunk> GetNextChunk() override {
    const auto& selected_names = config_.include_columns.empty()
                                     ? config_.column_names
                                     : config_.include_columns;
    std::vector<DataType> selected_types;
    selected_types.reserve(selected_names.size());
    for (const auto& name : selected_names) {
      auto iter = config_.column_types.find(name);
      if (iter == config_.column_types.end()) {
        selected_types.emplace_back(DataTypeId::kVarchar);
      } else {
        selected_types.push_back(iter->second);
      }
    }

    std::vector<std::shared_ptr<execution::IContextColumnBuilder>> builders;
    builders.reserve(selected_names.size());
    for (const auto& type : selected_types) {
      builders.push_back(execution::ColumnsUtils::create_builder(type));
    }

    size_t rows_in_chunk = 0;
    while (rows_in_chunk < chunk_size_) {
      std::string line;
      if (config_.json_array_input) {
        if (line_index_ >= lines_.size()) {
          break;
        }
        line = lines_[line_index_++];
      } else {
        if (!input_ || !std::getline(*input_, line)) {
          break;
        }
        if (line.empty()) {
          continue;
        }
      }

      rapidjson::Document doc;
      doc.Parse(line.c_str(), line.size());
      if (doc.HasParseError() || !doc.IsObject()) {
        THROW_IO_EXCEPTION("Invalid JSON object in file: " + file_path_);
      }

      for (size_t col = 0; col < selected_names.size(); ++col) {
        const auto& name = selected_names[col];
        if (!doc.HasMember(name.c_str())) {
          THROW_SCHEMA_MISMATCH(
              "Column '" + name + "' not found in JSON object in file: " +
              file_path_);
        }
        builders[col]->push_back_elem(
            parse_json_value(doc[name.c_str()], selected_types[col]));
      }
      ++rows_in_chunk;
    }

    if (rows_in_chunk == 0) {
      return nullptr;
    }

    auto chunk = std::make_shared<execution::DataChunk>();
    for (size_t col = 0; col < builders.size(); ++col) {
      chunk->set(static_cast<int>(col), builders[col]->finish());
    }
    return chunk;
  }

  int64_t RowNum() const override { return row_num_; }

 private:
  std::string file_path_;
  JsonReadConfig config_;
  int64_t row_num_ = 0;
  size_t chunk_size_ = kDefaultJsonChunkRows;
  std::vector<std::string> lines_;
  size_t line_index_ = 0;
  std::unique_ptr<std::ifstream> input_;
};

JsonReadConfig read_config_for_supplier(const JsonReadConfig& config) {
  JsonReadConfig read_config = config;
  read_config.include_columns = config.column_names;
  return read_config;
}

}  // namespace

JsonReader::JsonReader(std::shared_ptr<ReadSharedState> sharedState,
                       std::unique_ptr<JsonOptionsBuilder> optionsBuilder)
    : sharedState_(std::move(sharedState)),
      optionsBuilder_(std::move(optionsBuilder)) {}

JsonReader::~JsonReader() = default;

std::shared_ptr<IDataChunkSupplier> JsonReader::read() {
  if (!sharedState_ || !optionsBuilder_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("JsonReader state or builder is null");
  }

  auto config = optionsBuilder_->build();
  if (!optionsBuilder_->projectColumns(config)) {
    LOG(WARNING) << "Failed to set column projection, using all columns";
  }

  const auto& fileSchema = sharedState_->schema.file;
  ReadOptions readOpts;
  const bool use_batch_read = readOpts.batch_read.get(fileSchema.options);

  auto read_config = read_config_for_supplier(config);
  if (sharedState_->skipRows || !sharedState_->projectColumns.empty()) {
    read_config.include_columns = config.column_names;
  }

  const auto& paths = fileSchema.paths;
  if (paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers;
  suppliers.reserve(paths.size());
  for (const auto& path : paths) {
    suppliers.push_back(std::make_shared<JsonChunkSupplier>(path, read_config));
  }

  if (use_batch_read) {
    return batch_read(suppliers);
  }
  return full_read(suppliers, config);
}

std::shared_ptr<IDataChunkSupplier> JsonReader::full_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    const JsonReadConfig& output_config) {
  auto merged = read_all_chunks(suppliers);

  int expected_cols = sharedState_->columnNum();
  if (expected_cols > 0 &&
      static_cast<int>(merged.col_num()) != expected_cols &&
      sharedState_->projectColumns.empty()) {
    THROW_IO_EXCEPTION(
        "Column number mismatch between schema and JSON data, schema: " +
        std::to_string(expected_cols) + ", data: " +
        std::to_string(merged.col_num()));
  }

  auto filtered = filter_chunk(merged, sharedState_->skipRows,
                               output_config.column_names);
  auto projected = project_chunk(filtered, output_config.column_names,
                                 sharedState_->projectColumns.empty()
                                     ? output_config.include_columns
                                     : sharedState_->projectColumns);
  return std::make_shared<ChunkSupplierWrapper>(
      std::vector<std::shared_ptr<IDataChunkSupplier>>{},
      std::make_shared<execution::DataChunk>(std::move(projected)));
}

std::shared_ptr<IDataChunkSupplier> JsonReader::batch_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers) {
  if (suppliers.size() == 1) {
    return suppliers.front();
  }
  return std::make_shared<ChunkSupplierWrapper>(suppliers);
}

result<std::shared_ptr<EntrySchema>> JsonReader::inferSchema() {
  if (!sharedState_ || !optionsBuilder_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "JsonReader state or builder is null");
  }

  auto config = optionsBuilder_->build();
  const auto& paths = sharedState_->schema.file.paths;
  if (paths.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "No file paths provided");
  }

  JsonReadConfig sniff_config = config;
  sniff_config.include_columns.clear();
  if (config.column_names.empty()) {
    std::string sample_line;
    if (config.json_array_input) {
      auto lines = convert_json_array_to_lines(paths[0]);
      if (lines.empty()) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                            "Empty JSON array in file: " + paths[0]);
      }
      sample_line = lines.front();
    } else {
      std::ifstream input(paths[0]);
      while (std::getline(input, sample_line)) {
        if (!sample_line.empty()) {
          break;
        }
      }
    }
    rapidjson::Document doc;
    doc.Parse(sample_line.c_str(), sample_line.size());
    if (doc.HasParseError() || !doc.IsObject()) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to parse JSON object for schema inference");
    }
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
      sniff_config.column_names.push_back(it->name.GetString());
    }
  }

  sniff_config.include_columns = sniff_config.column_names;
  for (const auto& name : sniff_config.column_names) {
    sniff_config.column_types[name] = DataType(DataTypeId::kVarchar);
  }

  auto supplier = std::make_shared<JsonChunkSupplier>(paths[0], sniff_config);
  auto sample_chunk = supplier->GetNextChunk();
  if (!sample_chunk) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                        "Failed to read sample rows for schema inference");
  }

  auto entrySchema = std::make_shared<TableEntrySchema>();
  entrySchema->columnNames = sniff_config.column_names;
  entrySchema->columnTypes.reserve(sniff_config.column_names.size());

  NeuGTypeConverter converter;
  for (size_t col = 0; col < sniff_config.column_names.size(); ++col) {
    bool all_int = true;
    bool all_double = true;
    bool has_value = false;
    for (size_t row = 0; row < sample_chunk->row_num(); ++row) {
      auto value = sample_chunk->get(static_cast<int>(col))->get_elem(row);
      if (value.IsNull()) {
        continue;
      }
      has_value = true;
      const auto type_id = value.type().id();
      if (type_id != DataTypeId::kInt32 && type_id != DataTypeId::kInt64) {
        all_int = false;
      }
      if (type_id != DataTypeId::kDouble && type_id != DataTypeId::kFloat &&
          type_id != DataTypeId::kInt32 && type_id != DataTypeId::kInt64) {
        all_double = false;
      }
    }
    DataType inferred_type(DataTypeId::kVarchar);
    if (has_value && all_int) {
      inferred_type = DataType(DataTypeId::kInt64);
    } else if (has_value && all_double) {
      inferred_type = DataType(DataTypeId::kDouble);
    }
    entrySchema->columnTypes.push_back(converter.inferCommonType(inferred_type));
  }

  return entrySchema;
}

}  // namespace reader
}  // namespace neug
