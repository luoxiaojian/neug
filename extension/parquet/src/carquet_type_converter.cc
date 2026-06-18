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

#include <cstring>
#include <unordered_map>

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

std::shared_ptr<::common::DataType> makeListType(
    const std::shared_ptr<::common::DataType>& element_type) {
  auto dt = std::make_shared<::common::DataType>();
  auto* array = dt->mutable_array();
  *array->mutable_component_type() = *element_type;
  return dt;
}

std::shared_ptr<::common::DataType> makeMapType(
    const std::shared_ptr<::common::DataType>& key_type,
    const std::shared_ptr<::common::DataType>& value_type) {
  auto dt = std::make_shared<::common::DataType>();
  auto* map = dt->mutable_map();
  *map->mutable_key_type() = *key_type;
  *map->mutable_value_type() = *value_type;
  return dt;
}

bool isListLeafPath(const char* const* path, int32_t depth) {
  return depth >= 3 && std::strcmp(path[1], "list") == 0 &&
         std::strcmp(path[2], "element") == 0;
}

bool isMapKeyLeafPath(const char* const* path, int32_t depth) {
  return depth >= 3 && std::strcmp(path[1], "key_value") == 0 &&
         std::strcmp(path[2], "key") == 0;
}

bool isMapValueLeafPath(const char* const* path, int32_t depth) {
  return depth >= 3 && std::strcmp(path[1], "key_value") == 0 &&
         std::strcmp(path[2], "value") == 0;
}

bool isNestedLeafPath(const char* const* path, int32_t depth) {
  return isListLeafPath(path, depth) || isMapKeyLeafPath(path, depth) ||
         isMapValueLeafPath(path, depth);
}

std::vector<int32_t> collectStructLeavesForTopLevel(
    const carquet_schema_t* schema, const char* top_level_name) {
  std::vector<int32_t> leaves;
  const int32_t num_leaves = carquet_schema_num_columns(schema);
  for (int32_t leaf = 0; leaf < num_leaves; ++leaf) {
    const char* path[16];
    const int32_t depth = carquet_schema_column_path(schema, leaf, path, 16);
    if (depth <= 1 || path[0] == nullptr ||
        std::strcmp(path[0], top_level_name) != 0) {
      continue;
    }
    if (isNestedLeafPath(path, depth)) {
      continue;
    }
    leaves.push_back(leaf);
  }
  return leaves;
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
  const int32_t num_leaves = carquet_schema_num_columns(schema);
  std::unordered_map<std::string, std::vector<int32_t>> struct_leaves;
  std::unordered_map<std::string, int32_t> list_leaves;
  struct MapLeafPair {
    int32_t key = -1;
    int32_t value = -1;
  };
  std::unordered_map<std::string, MapLeafPair> map_leaves;
  std::vector<std::pair<std::string, int32_t>> primitive_columns;

  for (int32_t leaf = 0; leaf < num_leaves; ++leaf) {
    const char* path[16];
    const int32_t depth =
        carquet_schema_column_path(schema, leaf, path, 16);
    if (depth <= 0 || path[0] == nullptr) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Invalid Carquet column path at leaf " +
                              std::to_string(leaf));
    }

    const std::string top_level = path[0];
    if (depth == 1) {
      primitive_columns.emplace_back(top_level, leaf);
      continue;
    }
    if (isListLeafPath(path, depth)) {
      list_leaves[top_level] = leaf;
      continue;
    }
    if (isMapKeyLeafPath(path, depth)) {
      map_leaves[top_level].key = leaf;
      continue;
    }
    if (isMapValueLeafPath(path, depth)) {
      map_leaves[top_level].value = leaf;
      continue;
    }
    struct_leaves[top_level].push_back(leaf);
  }

  auto appendColumn = [&](const std::string& name,
                          const std::shared_ptr<::common::DataType>& type) {
    entry_schema->columnNames.push_back(name);
    entry_schema->columnTypes.push_back(type);
  };

  for (const auto& [name, leaf] : primitive_columns) {
    const auto physical = carquet_schema_column_type(schema, leaf);
    const auto* logical = leafLogicalType(schema, leaf);
    appendColumn(name, carquetColumnToCommonType(physical, logical));
  }

  for (const auto& [name, leaf] : list_leaves) {
    const auto physical = carquet_schema_column_type(schema, leaf);
    const auto* logical = leafLogicalType(schema, leaf);
    appendColumn(name, makeListType(carquetColumnToCommonType(physical, logical)));
  }

  for (const auto& [name, key_value] : map_leaves) {
    const int32_t key_leaf = key_value.key;
    const int32_t value_leaf = key_value.value;
    if (key_leaf < 0 || value_leaf < 0) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Incomplete map schema for column " + name);
    }
    const auto key_type = carquetColumnToCommonType(
        carquet_schema_column_type(schema, key_leaf),
        leafLogicalType(schema, key_leaf));
    const auto value_type = carquetColumnToCommonType(
        carquet_schema_column_type(schema, value_leaf),
        leafLogicalType(schema, value_leaf));
    appendColumn(name, makeMapType(key_type, value_type));
  }

  for (const auto& [name, leaves] : struct_leaves) {
    if (leaves.empty()) {
      continue;
    }
    auto tuple_type = std::make_shared<::common::DataType>();
    auto* tuple = tuple_type->mutable_tuple();
    for (int32_t leaf : leaves) {
      const auto physical = carquet_schema_column_type(schema, leaf);
      const auto* logical = leafLogicalType(schema, leaf);
      auto* field = tuple->add_component_types();
      *field = *carquetColumnToCommonType(physical, logical);
    }
    appendColumn(name, tuple_type);
  }

  if (entry_schema->columnNames.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                        "Carquet schema has no readable columns");
  }

  return entry_schema;
}

std::vector<int32_t> resolveCarquetColumnIndices(
    const carquet_schema_t* schema, const std::vector<std::string>& names) {
  std::vector<int32_t> indices;
  indices.reserve(names.size());
  for (const auto& name : names) {
    const int32_t idx = carquetSchemaFindTopLevelColumn(schema, name.c_str());
    if (idx < 0) {
      THROW_SCHEMA_MISMATCH("Column '" + name + "' not found in Parquet file");
    }
    indices.push_back(idx);
  }
  return indices;
}

int32_t carquetSchemaFindTopLevelColumn(const carquet_schema_t* schema,
                                        const char* name) {
  if (schema == nullptr || name == nullptr) {
    return -1;
  }
  const int32_t num_leaves = carquet_schema_num_columns(schema);
  for (int32_t leaf = 0; leaf < num_leaves; ++leaf) {
    const char* path[16];
    const int32_t depth = carquet_schema_column_path(schema, leaf, path, 16);
    if (depth > 0 && path[0] != nullptr && std::strcmp(path[0], name) == 0) {
      return leaf;
    }
  }
  return -1;
}

CarquetColumnLayout carquetClassifyLeafColumn(const carquet_schema_t* schema,
                                              int32_t leaf_index) {
  const char* path[16];
  const int32_t depth =
      carquet_schema_column_path(schema, leaf_index, path, 16);
  if (depth <= 1) {
    return CarquetColumnLayout::kFlat;
  }
  if (isListLeafPath(path, depth)) {
    return CarquetColumnLayout::kList;
  }
  if (isMapKeyLeafPath(path, depth)) {
    return CarquetColumnLayout::kMap;
  }
  if (isMapValueLeafPath(path, depth)) {
    return CarquetColumnLayout::kMap;
  }
  return CarquetColumnLayout::kStruct;
}

std::vector<CarquetProjectedColumn> resolveCarquetProjectedColumns(
    const carquet_schema_t* schema, const std::vector<std::string>& names) {
  std::vector<CarquetProjectedColumn> projected;
  projected.reserve(names.size());
  for (const auto& name : names) {
    const int32_t leaf = carquetSchemaFindTopLevelColumn(schema, name.c_str());
    if (leaf < 0) {
      THROW_SCHEMA_MISMATCH("Column '" + name + "' not found in Parquet file");
    }
    CarquetProjectedColumn column;
    column.leaf_index = leaf;
    column.layout = carquetClassifyLeafColumn(schema, leaf);
    column.max_def_level = carquet_schema_max_def_level(schema, leaf);
    column.max_rep_level = carquet_schema_max_rep_level(schema, leaf);

    if (column.layout == CarquetColumnLayout::kMap) {
      const char* path[16];
      const int32_t depth = carquet_schema_column_path(schema, leaf, path, 16);
      if (depth > 0 && path[0] != nullptr) {
        const std::string top = path[0];
        const int32_t num_leaves = carquet_schema_num_columns(schema);
        for (int32_t other = 0; other < num_leaves; ++other) {
          const char* other_path[16];
          const int32_t other_depth =
              carquet_schema_column_path(schema, other, other_path, 16);
          if (other_depth <= 0 || other_path[0] == nullptr ||
              top != other_path[0]) {
            continue;
          }
          if (isMapKeyLeafPath(other_path, other_depth)) {
            column.map_key_leaf = other;
          } else if (isMapValueLeafPath(other_path, other_depth)) {
            column.map_value_leaf = other;
          }
        }
      }
    }

    if (column.layout == CarquetColumnLayout::kStruct) {
      column.struct_leaf_indices =
          collectStructLeavesForTopLevel(schema, name.c_str());
    }

    projected.push_back(column);
  }
  return projected;
}

}  // namespace reader
}  // namespace neug
