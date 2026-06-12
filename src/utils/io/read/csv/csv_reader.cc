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

#include "neug/utils/io/reader.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <memory>
#include <stack>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "neug/execution/common/columns/chunk_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/operator_precedence.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/read/common/type_converter.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {
namespace {

execution::Value proto_value_to_execution(const ::common::Value& value) {
  switch (value.item_case()) {
  case ::common::Value::kBoolean:
    return execution::Value::BOOLEAN(value.boolean());
  case ::common::Value::kI32:
    return execution::Value::INT32(value.i32());
  case ::common::Value::kI64:
    return execution::Value::INT64(value.i64());
  case ::common::Value::kU32:
    return execution::Value::UINT32(value.u32());
  case ::common::Value::kU64:
    return execution::Value::UINT64(value.u64());
  case ::common::Value::kF32:
    return execution::Value::FLOAT(value.f32());
  case ::common::Value::kF64:
    return execution::Value::DOUBLE(value.f64());
  case ::common::Value::kStr:
    return execution::Value::STRING(value.str());
  default:
    THROW_CONVERSION_EXCEPTION("Unsupported constant type in CSV row filter");
  }
}

bool compare_values(const ::common::Logical& logical,
                    const execution::Value& left,
                    const execution::Value& right) {
  switch (logical) {
  case ::common::Logical::GT:
    return right < left;
  case ::common::Logical::GE:
    return !(left < right);
  case ::common::Logical::LT:
    return left < right;
  case ::common::Logical::LE:
    return !(right < left);
  case ::common::Logical::EQ:
    return left == right;
  case ::common::Logical::NE:
    return !(left == right);
  case ::common::Logical::AND:
    return left.GetValue<bool>() && right.GetValue<bool>();
  case ::common::Logical::OR:
    return left.GetValue<bool>() || right.GetValue<bool>();
  case ::common::Logical::NOT:
    return !left.GetValue<bool>();
  default:
    THROW_NOT_IMPLEMENTED_EXCEPTION("Unsupported logical operator in CSV filter");
  }
}

class CsvRowFilter {
 public:
  CsvRowFilter(const ::common::Expression& expr,
               const std::unordered_map<std::string, int>& column_index)
      : column_index_(column_index) {
    compile(expr);
  }

  bool eval(const execution::DataChunk& chunk, size_t row) const {
    if (!evaluator_) {
      return true;
    }
    return evaluator_(chunk, row);
  }

 private:
  using ValueFn =
      std::function<execution::Value(const execution::DataChunk&, size_t)>;
  using EvalFn = std::function<bool(const execution::DataChunk&, size_t)>;

  void compile(const ::common::Expression& expr) {
    std::stack<ValueFn> value_stack;
    std::stack<::common::ExprOpr> op_stack;

    auto apply_operator = [&](const ::common::ExprOpr& opr) {
      if (opr.item_case() != ::common::ExprOpr::kLogical) {
        THROW_NOT_IMPLEMENTED_EXCEPTION(
            "Unsupported operator in CSV row filter");
      }
      if (opr.logical() == ::common::Logical::NOT) {
        if (value_stack.empty()) {
          THROW_INVALID_ARGUMENT_EXCEPTION("Not enough operands for NOT");
        }
        auto operand = value_stack.top();
        value_stack.pop();
        value_stack.push([operand](const execution::DataChunk& chunk, size_t row) {
          return execution::Value::BOOLEAN(
              compare_values(::common::Logical::NOT, operand(chunk, row),
                             execution::Value::BOOLEAN(false)));
        });
        return;
      }
      if (value_stack.size() < 2) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Not enough operands for binary logical operation");
      }
      auto right_fn = value_stack.top();
      value_stack.pop();
      auto left_fn = value_stack.top();
      value_stack.pop();
      auto logical = opr.logical();
      value_stack.push([left_fn, right_fn, logical](
                           const execution::DataChunk& chunk, size_t row) {
        auto left_val = left_fn(chunk, row);
        auto right_val = right_fn(chunk, row);
        if (logical == ::common::Logical::AND || logical == ::common::Logical::OR) {
          return execution::Value::BOOLEAN(
              compare_values(logical, execution::Value::BOOLEAN(left_val.GetValue<bool>()),
                             execution::Value::BOOLEAN(right_val.GetValue<bool>())));
        }
        return execution::Value::BOOLEAN(compare_values(logical, left_val, right_val));
      });
    };

    for (int i = 0; i < expr.operators_size(); ++i) {
      const auto& opr = expr.operators(i);
      switch (opr.item_case()) {
      case ::common::ExprOpr::kConst: {
        auto value = proto_value_to_execution(opr.const_());
        value_stack.push([value](const execution::DataChunk&, size_t) { return value; });
        break;
      }
      case ::common::ExprOpr::kVar: {
        const std::string& column_name = opr.var().tag().name();
        auto iter = column_index_.find(column_name);
        if (iter == column_index_.end()) {
          THROW_INVALID_ARGUMENT_EXCEPTION("Filter column not found: " +
                                           column_name);
        }
        int col_idx = iter->second;
        value_stack.push([col_idx](const execution::DataChunk& chunk, size_t row) {
          auto col = chunk.get(col_idx);
          if (!col) {
            THROW_RUNTIME_ERROR("Missing filter column at index " +
                                std::to_string(col_idx));
          }
          return col->get_elem(row);
        });
        break;
      }
      case ::common::ExprOpr::kBrace: {
        if (opr.brace() == ::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
          op_stack.push(opr);
        } else {
          while (!op_stack.empty() &&
                 op_stack.top().item_case() != ::common::ExprOpr::kBrace) {
            apply_operator(op_stack.top());
            op_stack.pop();
          }
          if (op_stack.empty()) {
            THROW_INVALID_ARGUMENT_EXCEPTION("Mismatched parentheses in filter");
          }
          op_stack.pop();
        }
        break;
      }
      case ::common::ExprOpr::kLogical: {
        int current_prec = OperatorPrecedence::getPrecedence(opr);
        while (!op_stack.empty() &&
               op_stack.top().item_case() != ::common::ExprOpr::kBrace &&
               OperatorPrecedence::getPrecedence(op_stack.top()) <=
                   current_prec) {
          apply_operator(op_stack.top());
          op_stack.pop();
        }
        op_stack.push(opr);
        break;
      }
      default:
        THROW_NOT_IMPLEMENTED_EXCEPTION("Unsupported token in CSV row filter");
      }
    }

    while (!op_stack.empty()) {
      if (op_stack.top().item_case() == ::common::ExprOpr::kBrace) {
        THROW_INVALID_ARGUMENT_EXCEPTION("Mismatched parentheses in filter");
      }
      apply_operator(op_stack.top());
      op_stack.pop();
    }

    if (value_stack.empty()) {
      evaluator_ = nullptr;
      return;
    }
    if (value_stack.size() != 1) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Invalid filter expression");
    }
    auto value_fn = value_stack.top();
    evaluator_ = [value_fn](const execution::DataChunk& chunk, size_t row) {
      return value_fn(chunk, row).GetValue<bool>();
    };
  }

  std::unordered_map<std::string, int> column_index_;
  EvalFn evaluator_;
};

execution::DataChunk read_all_chunks(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers) {
  execution::DataChunk merged;
  for (const auto& supplier : suppliers) {
    while (true) {
      auto chunk = supplier->GetNextChunk();
      if (!chunk) {
        break;
      }
      if (merged.row_num() == 0) {
        merged = *chunk;
      } else {
        merged = merged.union_chunk(*chunk);
      }
    }
  }
  return merged;
}

void build_name_to_index(const std::vector<std::string>& column_names,
                         std::unordered_map<std::string, int>* name_to_index) {
  for (size_t i = 0; i < column_names.size(); ++i) {
    (*name_to_index)[column_names[i]] = static_cast<int>(i);
  }
}

execution::DataChunk filter_chunk(
    const execution::DataChunk& input,
    const std::shared_ptr<::common::Expression>& filter_expr,
    const std::vector<std::string>& column_names) {
  if (!filter_expr || input.row_num() == 0) {
    return input;
  }

  std::unordered_map<std::string, int> name_to_index;
  build_name_to_index(column_names, &name_to_index);
  CsvRowFilter filter(*filter_expr, name_to_index);

  std::vector<size_t> keep_offsets;
  keep_offsets.reserve(input.row_num());
  for (size_t row = 0; row < input.row_num(); ++row) {
    if (filter.eval(input, row)) {
      keep_offsets.push_back(row);
    }
  }

  execution::DataChunk filtered = input;
  filtered.reshuffle(keep_offsets);
  return filtered;
}

execution::DataChunk project_chunk(const execution::DataChunk& input,
                                   const std::vector<std::string>& column_names,
                                   const std::vector<std::string>& project_columns) {
  if (project_columns.empty()) {
    return input;
  }

  std::unordered_map<std::string, int> name_to_index;
  build_name_to_index(column_names, &name_to_index);

  execution::DataChunk projected;
  for (size_t i = 0; i < project_columns.size(); ++i) {
    auto iter = name_to_index.find(project_columns[i]);
    if (iter == name_to_index.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Project column not found: " +
                                       project_columns[i]);
    }
    projected.set(static_cast<int>(i), input.get(iter->second));
  }
  return projected;
}

CsvReadConfig read_config_for_supplier(const CsvReadConfig& config) {
  CsvReadConfig read_config = config;
  read_config.include_columns = config.column_names;
  return read_config;
}

}  // namespace

CsvReader::CsvReader(std::shared_ptr<ReadSharedState> sharedState,
                     std::unique_ptr<CsvOptionsBuilder> optionsBuilder)
    : sharedState_(std::move(sharedState)),
      optionsBuilder_(std::move(optionsBuilder)) {}

CsvReader::~CsvReader() = default;

void CsvReader::read(std::shared_ptr<ReadLocalState> /*localState*/,
                     execution::Context& ctx) {
  if (!sharedState_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  if (!optionsBuilder_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Options builder is null");
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
    suppliers.push_back(
        std::make_shared<CSVChunkSupplier>(path, read_config));
  }

  if (use_batch_read) {
    batch_read(suppliers, ctx);
  } else {
    full_read(suppliers, ctx, config);
  }
}

void CsvReader::full_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    execution::Context& output, const CsvReadConfig& output_config) {
  auto merged = read_all_chunks(suppliers);

  int expected_cols = sharedState_->columnNum();
  if (expected_cols > 0 &&
      static_cast<int>(merged.col_num()) != expected_cols &&
      sharedState_->projectColumns.empty()) {
    THROW_IO_EXCEPTION(
        "Column number mismatch between schema and CSV data, schema: " +
        std::to_string(expected_cols) + ", data: " +
        std::to_string(merged.col_num()));
  }

  auto filtered = filter_chunk(merged, sharedState_->skipRows,
                               output_config.column_names);
  auto projected = project_chunk(filtered, output_config.column_names,
                                 sharedState_->projectColumns.empty()
                                     ? output_config.include_columns
                                     : sharedState_->projectColumns);

  output.clear();
  output.append_chunk(std::move(projected));
}

void CsvReader::batch_read(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
    execution::Context& output) {
  std::shared_ptr<IDataChunkSupplier> batch_supplier;
  if (suppliers.size() == 1) {
    batch_supplier = suppliers.front();
  } else {
    batch_supplier = std::make_shared<ChunkSupplierWrapper>(suppliers);
  }

  int num_cols = sharedState_->columnNum();
  if (num_cols <= 0) {
    num_cols =
        static_cast<int>(optionsBuilder_->build().include_columns.size());
  }

  output.clear();
  execution::DataChunk chunk;
  for (int i = 0; i < num_cols; ++i) {
    execution::ChunkStreamContextColumnBuilder builder({batch_supplier});
    chunk.set(i, builder.finish());
  }
  output.append_chunk(std::move(chunk));
}

result<std::shared_ptr<EntrySchema>> CsvReader::inferSchema() {
  if (!sharedState_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "SharedState is null");
  }
  if (!optionsBuilder_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Options builder is null");
  }

  auto config = optionsBuilder_->build();
  const auto& paths = sharedState_->schema.file.paths;
  if (paths.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "No file paths provided");
  }

  ReadOptions readOpts;
  const bool autogenerate =
      readOpts.autogenerate_column_names.get(
          sharedState_->schema.file.options);

  if (config.column_names.empty() && !autogenerate) {
    config.column_names = read_header(paths[0], config);
  } else if (config.column_names.empty() && autogenerate) {
    std::ifstream input(paths[0]);
    if (!input) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to open CSV file for schema inference");
    }
    std::string line;
    if (!std::getline(input, line)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to read first row for schema inference");
    }
    size_t column_count = 1;
    for (char ch : line) {
      if (ch == config.delimiter) {
        ++column_count;
      }
    }
    config.column_names.reserve(column_count);
    for (size_t i = 0; i < column_count; ++i) {
      config.column_names.push_back("f" + std::to_string(i));
    }
  }

  if (!autogenerate) {
    std::unordered_set<std::string> seen;
    for (const auto& name : config.column_names) {
      if (!seen.insert(name).second) {
        RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                            "Duplicate column name found: " + name);
      }
    }
  }

  CsvReadConfig sniff_config = config;
  sniff_config.include_columns = config.column_names;
  for (const auto& name : config.column_names) {
    sniff_config.column_types[name] = DataType(DataTypeId::kVarchar);
  }

  auto supplier = std::make_shared<CSVChunkSupplier>(paths[0], sniff_config);
  auto sample_chunk = supplier->GetNextChunk();
  if (!sample_chunk) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                        "Failed to read sample rows for schema inference");
  }

  auto entrySchema = std::make_shared<TableEntrySchema>();
  entrySchema->columnNames = config.column_names;
  entrySchema->columnTypes.reserve(config.column_names.size());

  NeuGTypeConverter converter;
  for (size_t col = 0; col < config.column_names.size(); ++col) {
    bool all_int = true;
    bool all_double = true;
    bool has_value = false;
    for (size_t row = 0; row < sample_chunk->row_num(); ++row) {
      auto value = sample_chunk->get(static_cast<int>(col))->get_elem(row);
      if (value.IsNull()) {
        continue;
      }
      has_value = true;
      const std::string& token = value.GetValue<std::string>();
      if (token.empty()) {
        all_int = false;
        all_double = false;
        continue;
      }
      char* end = nullptr;
      std::strtol(token.c_str(), &end, 10);
      if (end != token.c_str() + token.size()) {
        all_int = false;
      }
      end = nullptr;
      std::strtod(token.c_str(), &end);
      if (end != token.c_str() + token.size()) {
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
