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

#include "parquet/carquet_type_converter.h"

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {
namespace {

std::shared_ptr<::common::DataType> makePrimitive(
    ::common::PrimitiveType type) {
  auto dt = std::make_shared<::common::DataType>();
  dt->set_primitive_type(type);
  return dt;
}

std::shared_ptr<::common::DataType> makeStringType() {
  auto dt = std::make_shared<::common::DataType>();
  auto strType = std::make_unique<::common::String>();
  auto varChar = std::make_unique<::common::String::VarChar>();
  strType->set_allocated_var_char(varChar.release());
  dt->set_allocated_string(strType.release());
  return dt;
}

std::shared_ptr<::common::DataType> makeDateType() {
  auto dt = std::make_shared<::common::DataType>();
  auto temporal = std::make_unique<::common::Temporal>();
  temporal->set_allocated_date(new ::common::Temporal::Date());
  dt->set_allocated_temporal(temporal.release());
  return dt;
}

std::shared_ptr<::common::DataType> makeTimestampType() {
  auto dt = std::make_shared<::common::DataType>();
  auto temporal = std::make_unique<::common::Temporal>();
  temporal->set_allocated_timestamp(new ::common::Temporal::Timestamp());
  dt->set_allocated_temporal(temporal.release());
  return dt;
}

const carquet_logical_type_t* leafLogicalType(const carquet_schema_t* schema,
                                              int32_t leaf_index) {
  int32_t leaf = 0;
  const int32_t num_elements = carquet_schema_num_elements(schema);
  for (int32_t i = 0; i < num_elements; ++i) {
    const auto* node = carquet_schema_get_element(schema, i);
    if (node == nullptr || !carquet_schema_node_is_leaf(node)) {
      continue;
    }
    if (leaf == leaf_index) {
      return carquet_schema_node_logical_type(node);
    }
    ++leaf;
  }
  return nullptr;
}

}  // namespace

const carquet_logical_type_t* carquetLeafLogicalType(
    const carquet_schema_t* schema, int32_t leaf_index) {
  return leafLogicalType(schema, leaf_index);
}

std::shared_ptr<::common::DataType> carquetColumnToCommonType(
    carquet_physical_type_t physical, const carquet_logical_type_t* logical) {
  if (logical != nullptr) {
    switch (logical->id) {
    case CARQUET_LOGICAL_STRING:
    case CARQUET_LOGICAL_JSON:
    case CARQUET_LOGICAL_ENUM:
    case CARQUET_LOGICAL_UUID:
      return makeStringType();
    case CARQUET_LOGICAL_DATE:
      return makeDateType();
    case CARQUET_LOGICAL_TIMESTAMP:
    case CARQUET_LOGICAL_TIME:
      return makeTimestampType();
    case CARQUET_LOGICAL_INTEGER:
      if (logical->params.integer.is_signed) {
        if (logical->params.integer.bit_width <= 32) {
          return makePrimitive(::common::PrimitiveType::DT_SIGNED_INT32);
        }
        return makePrimitive(::common::PrimitiveType::DT_SIGNED_INT64);
      }
      if (logical->params.integer.bit_width <= 32) {
        return makePrimitive(::common::PrimitiveType::DT_UNSIGNED_INT32);
      }
      return makePrimitive(::common::PrimitiveType::DT_UNSIGNED_INT64);
    case CARQUET_LOGICAL_DECIMAL:
      return makePrimitive(::common::PrimitiveType::DT_DOUBLE);
    default:
      break;
    }
  }

  switch (physical) {
  case CARQUET_PHYSICAL_BOOLEAN:
    return makePrimitive(::common::PrimitiveType::DT_BOOL);
  case CARQUET_PHYSICAL_INT32:
    return makePrimitive(::common::PrimitiveType::DT_SIGNED_INT32);
  case CARQUET_PHYSICAL_INT64:
    return makePrimitive(::common::PrimitiveType::DT_SIGNED_INT64);
  case CARQUET_PHYSICAL_FLOAT:
    return makePrimitive(::common::PrimitiveType::DT_FLOAT);
  case CARQUET_PHYSICAL_DOUBLE:
    return makePrimitive(::common::PrimitiveType::DT_DOUBLE);
  case CARQUET_PHYSICAL_BYTE_ARRAY:
  case CARQUET_PHYSICAL_FIXED_LEN_BYTE_ARRAY:
    return makeStringType();
  case CARQUET_PHYSICAL_INT96:
    return makeTimestampType();
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported Carquet physical type: " +
        std::string(carquet_physical_type_name(physical)));
  }
}

result<std::shared_ptr<EntrySchema>> carquetSchemaToEntrySchema(
    const carquet_schema_t* schema) {
  if (schema == nullptr) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Carquet schema is null");
  }

  auto entry_schema = std::make_shared<TableEntrySchema>();
  const int32_t num_columns = carquet_schema_num_columns(schema);
  entry_schema->columnNames.reserve(num_columns);
  entry_schema->columnTypes.reserve(num_columns);

  for (int32_t i = 0; i < num_columns; ++i) {
    const char* name = carquet_schema_column_name(schema, i);
    if (name == nullptr) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Carquet column name is null at index " +
                              std::to_string(i));
    }
    entry_schema->columnNames.emplace_back(name);
    const auto physical = carquet_schema_column_type(schema, i);
    const auto* logical = leafLogicalType(schema, i);
    entry_schema->columnTypes.push_back(
        carquetColumnToCommonType(physical, logical));
  }

  return entry_schema;
}

std::vector<int32_t> resolveCarquetColumnIndices(
    const carquet_schema_t* schema, const std::vector<std::string>& names) {
  std::vector<int32_t> indices;
  indices.reserve(names.size());
  for (const auto& name : names) {
    const int32_t idx = carquet_schema_find_column(schema, name.c_str());
    if (idx < 0) {
      THROW_SCHEMA_MISMATCH("Column '" + name + "' not found in Parquet file");
    }
    indices.push_back(idx);
  }
  return indices;
}

}  // namespace reader
}  // namespace neug
