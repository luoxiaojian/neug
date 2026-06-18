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

#include "parquet/carquet_export_type_converter.h"

#include <algorithm>
#include <cstring>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/vfs/file_system.h"

namespace neug {
namespace reader {
namespace {

constexpr const char* kFilePrefix = "file://";

struct CarquetPhysicalSpec {
  carquet_physical_type_t physical = CARQUET_PHYSICAL_BYTE_ARRAY;
  carquet_logical_type_t logical{};
  bool has_logical = false;
  int32_t type_length = 0;
};

std::string normalizeLocalPath(const std::string& path) {
  if (path.rfind(kFilePrefix, 0) == 0) {
    std::string local_path = path.substr(std::strlen(kFilePrefix));
    if (local_path.empty() || local_path[0] != '/') {
      local_path = "/" + local_path;
    }
    return local_path;
  }
  return path;
}

CarquetPhysicalSpec stringSpec() {
  CarquetPhysicalSpec spec;
  spec.physical = CARQUET_PHYSICAL_BYTE_ARRAY;
  spec.logical.id = CARQUET_LOGICAL_STRING;
  spec.has_logical = true;
  return spec;
}

CarquetPhysicalSpec timestampSpec() {
  CarquetPhysicalSpec spec;
  spec.physical = CARQUET_PHYSICAL_INT64;
  spec.logical.id = CARQUET_LOGICAL_TIMESTAMP;
  spec.logical.params.timestamp.unit = CARQUET_TIME_UNIT_MICROS;
  spec.logical.params.timestamp.is_adjusted_to_utc = true;
  spec.has_logical = true;
  return spec;
}

CarquetPhysicalSpec dateSpec() {
  CarquetPhysicalSpec spec;
  spec.physical = CARQUET_PHYSICAL_INT32;
  spec.logical.id = CARQUET_LOGICAL_DATE;
  spec.has_logical = true;
  return spec;
}

CarquetPhysicalSpec specFromProtoArray(const Array& proto_array) {
  if (proto_array.has_int32_array()) {
    CarquetPhysicalSpec spec;
    spec.physical = CARQUET_PHYSICAL_INT32;
    return spec;
  }
  if (proto_array.has_int64_array()) {
    CarquetPhysicalSpec spec;
    spec.physical = CARQUET_PHYSICAL_INT64;
    return spec;
  }
  if (proto_array.has_uint32_array()) {
    CarquetPhysicalSpec spec;
    spec.physical = CARQUET_PHYSICAL_INT32;
    spec.logical.id = CARQUET_LOGICAL_INTEGER;
    spec.logical.params.integer.bit_width = 32;
    spec.logical.params.integer.is_signed = false;
    spec.has_logical = true;
    return spec;
  }
  if (proto_array.has_uint64_array()) {
    CarquetPhysicalSpec spec;
    spec.physical = CARQUET_PHYSICAL_INT64;
    spec.logical.id = CARQUET_LOGICAL_INTEGER;
    spec.logical.params.integer.bit_width = 64;
    spec.logical.params.integer.is_signed = false;
    spec.has_logical = true;
    return spec;
  }
  if (proto_array.has_float_array()) {
    CarquetPhysicalSpec spec;
    spec.physical = CARQUET_PHYSICAL_FLOAT;
    return spec;
  }
  if (proto_array.has_double_array()) {
    CarquetPhysicalSpec spec;
    spec.physical = CARQUET_PHYSICAL_DOUBLE;
    return spec;
  }
  if (proto_array.has_bool_array()) {
    CarquetPhysicalSpec spec;
    spec.physical = CARQUET_PHYSICAL_BOOLEAN;
    return spec;
  }
  if (proto_array.has_string_array()) {
    return stringSpec();
  }
  if (proto_array.has_date_array()) {
    return dateSpec();
  }
  if (proto_array.has_timestamp_array()) {
    return timestampSpec();
  }
  if (proto_array.has_interval_array()) {
    return stringSpec();
  }
  if (proto_array.has_list_array()) {
    const auto& list_arr = proto_array.list_array();
    if (list_arr.has_elements()) {
      return specFromProtoArray(list_arr.elements());
    }
    return stringSpec();
  }
  if (proto_array.has_struct_array()) {
    return stringSpec();
  }
  if (proto_array.has_vertex_array() || proto_array.has_edge_array() ||
      proto_array.has_path_array()) {
    return stringSpec();
  }
  return stringSpec();
}

CarquetPhysicalSpec specFromCommonDataType(const ::common::DataType& dt) {
  if (dt.item_case() == ::common::DataType::kPrimitiveType) {
    switch (dt.primitive_type()) {
    case ::common::PrimitiveType::DT_SIGNED_INT32: {
      CarquetPhysicalSpec spec;
      spec.physical = CARQUET_PHYSICAL_INT32;
      return spec;
    }
    case ::common::PrimitiveType::DT_SIGNED_INT64: {
      CarquetPhysicalSpec spec;
      spec.physical = CARQUET_PHYSICAL_INT64;
      return spec;
    }
    case ::common::PrimitiveType::DT_UNSIGNED_INT32: {
      CarquetPhysicalSpec spec;
      spec.physical = CARQUET_PHYSICAL_INT32;
      spec.logical.id = CARQUET_LOGICAL_INTEGER;
      spec.logical.params.integer.bit_width = 32;
      spec.logical.params.integer.is_signed = false;
      spec.has_logical = true;
      return spec;
    }
    case ::common::PrimitiveType::DT_UNSIGNED_INT64: {
      CarquetPhysicalSpec spec;
      spec.physical = CARQUET_PHYSICAL_INT64;
      spec.logical.id = CARQUET_LOGICAL_INTEGER;
      spec.logical.params.integer.bit_width = 64;
      spec.logical.params.integer.is_signed = false;
      spec.has_logical = true;
      return spec;
    }
    case ::common::PrimitiveType::DT_FLOAT: {
      CarquetPhysicalSpec spec;
      spec.physical = CARQUET_PHYSICAL_FLOAT;
      return spec;
    }
    case ::common::PrimitiveType::DT_DOUBLE: {
      CarquetPhysicalSpec spec;
      spec.physical = CARQUET_PHYSICAL_DOUBLE;
      return spec;
    }
    case ::common::PrimitiveType::DT_BOOL: {
      CarquetPhysicalSpec spec;
      spec.physical = CARQUET_PHYSICAL_BOOLEAN;
      return spec;
    }
    default:
      break;
    }
  }
  if (dt.item_case() == ::common::DataType::kString) {
    return stringSpec();
  }
  if (dt.item_case() == ::common::DataType::kTemporal) {
    if (dt.temporal().item_case() == ::common::Temporal::kDate) {
      return dateSpec();
    }
    if (dt.temporal().item_case() == ::common::Temporal::kTimestamp) {
      return timestampSpec();
    }
  }
  return stringSpec();
}

carquet_status_t addMapSchemaFromDeclaredType(
    carquet_schema_t* schema, const char* name,
    const ::common::DataType& map_type, int32_t parent_index,
    CarquetExportColumn* column_out) {
  if (map_type.item_case() != ::common::DataType::kMap) {
    return CARQUET_ERROR_INVALID_ARGUMENT;
  }
  const CarquetPhysicalSpec key_spec =
      specFromCommonDataType(map_type.map().key_type());
  const CarquetPhysicalSpec val_spec =
      specFromCommonDataType(map_type.map().value_type());
  const int32_t base_leaves = carquet_schema_num_columns(schema);
  const int32_t map_group = carquet_schema_add_map(
      schema, name, key_spec.physical,
      key_spec.has_logical ? &key_spec.logical : nullptr, key_spec.type_length,
      val_spec.physical, val_spec.has_logical ? &val_spec.logical : nullptr,
      val_spec.type_length, CARQUET_REPETITION_OPTIONAL, parent_index);
  if (map_group < 0) {
    return CARQUET_ERROR_INVALID_ARGUMENT;
  }
  if (column_out != nullptr) {
    column_out->is_map = true;
    column_out->map_key_leaf = base_leaves;
    column_out->map_value_leaf = base_leaves + 1;
  }
  return CARQUET_OK;
}

carquet_status_t addLeafColumn(carquet_schema_t* schema, const char* name,
                               const CarquetPhysicalSpec& spec,
                               int32_t parent_index, int32_t* leaf_index) {
  const auto* logical = spec.has_logical ? &spec.logical : nullptr;
  const auto status = carquet_schema_add_column(
      schema, name, spec.physical, logical, CARQUET_REPETITION_OPTIONAL,
      spec.type_length, parent_index);
  if (status != CARQUET_OK) {
    return status;
  }
  if (leaf_index != nullptr) {
    *leaf_index = carquet_schema_num_columns(schema) - 1;
  }
  return CARQUET_OK;
}

carquet_status_t addSchemaFromProtoArray(
    carquet_schema_t* schema, const char* name, const Array& proto_array,
    int32_t parent_index, CarquetExportColumn* column_out) {
  if (proto_array.has_list_array()) {
    const auto& list_arr = proto_array.list_array();
    const Array& elements =
        list_arr.has_elements() ? list_arr.elements() : proto_array;
    const CarquetPhysicalSpec element_spec = specFromProtoArray(elements);

    const int32_t list_group = carquet_schema_add_list(
        schema, name, element_spec.physical,
        element_spec.has_logical ? &element_spec.logical : nullptr,
        CARQUET_REPETITION_OPTIONAL, element_spec.type_length, parent_index);
    if (list_group < 0) {
      return CARQUET_ERROR_INVALID_ARGUMENT;
    }
    if (column_out != nullptr) {
      column_out->leaf_index = carquet_schema_num_columns(schema) - 1;
      column_out->is_list = true;
    }
    return CARQUET_OK;
  }

  if (proto_array.has_struct_array()) {
    const auto& struct_arr = proto_array.struct_array();
    const int32_t group_idx = carquet_schema_add_group(
        schema, name, CARQUET_REPETITION_OPTIONAL, parent_index);
    if (group_idx < 0) {
      return CARQUET_ERROR_INVALID_ARGUMENT;
    }
    if (column_out != nullptr) {
      column_out->is_struct = true;
    }
    for (int i = 0; i < struct_arr.fields_size(); ++i) {
      const std::string field_name = "field_" + std::to_string(i);
      int32_t leaf_index = -1;
      const auto status = addSchemaFromProtoArray(
          schema, field_name.c_str(), struct_arr.fields(i), group_idx,
          nullptr);
      if (status != CARQUET_OK) {
        return status;
      }
      leaf_index = carquet_schema_num_columns(schema) - 1;
      if (column_out != nullptr) {
        column_out->struct_leaf_indices.push_back(leaf_index);
      }
    }
    return CARQUET_OK;
  }

  const CarquetPhysicalSpec spec = specFromProtoArray(proto_array);
  return addLeafColumn(schema, name, spec, parent_index,
                       column_out != nullptr ? &column_out->leaf_index
                                             : nullptr);
}

std::string columnNameFromResponse(const QueryResponse* table,
                                   const std::shared_ptr<EntrySchema>& entry_schema,
                                   int column_index) {
  if (column_index < table->schema().name_size()) {
    return table->schema().name(column_index);
  }
  if (entry_schema &&
      column_index < static_cast<int>(entry_schema->columnNames.size())) {
    return entry_schema->columnNames[static_cast<size_t>(column_index)];
  }
  return "col_" + std::to_string(column_index);
}

carquet_compression_t parseCarquetCompression(const std::string& codec_in) {
  std::string codec = codec_in;
  std::transform(codec.begin(), codec.end(), codec.begin(), ::tolower);
  if (codec == "none" || codec == "uncompressed") {
    return CARQUET_COMPRESSION_UNCOMPRESSED;
  }
  if (codec == "snappy") {
    return CARQUET_COMPRESSION_SNAPPY;
  }
  if (codec == "zlib" || codec == "gzip") {
    return CARQUET_COMPRESSION_GZIP;
  }
  if (codec == "zstd" || codec == "zstandard") {
    return CARQUET_COMPRESSION_ZSTD;
  }
  THROW_INVALID_ARGUMENT_EXCEPTION(
      "Unsupported compression codec: " + codec +
      ". Supported: none, snappy, gzip (zlib), zstd");
}

}  // namespace

std::string resolveCarquetExportPath(fsys::FileSystem& /*fs*/,
                                     const std::string& path) {
  return normalizeLocalPath(path);
}

carquet_writer_options_t buildCarquetWriterOptions(
    const ParquetWriteOptions& options) {
  carquet_writer_options_t writer_opts;
  carquet_writer_options_init(&writer_opts);
  writer_opts.compression = parseCarquetCompression(options.compression);
  writer_opts.row_group_size = options.row_group_size;
  writer_opts.dictionary_encoding = options.dictionary_encoding
                                        ? CARQUET_ENCODING_RLE_DICTIONARY
                                        : CARQUET_ENCODING_PLAIN;
  return writer_opts;
}

carquet_schema_t* buildCarquetSchemaFromQueryResponse(
    const QueryResponse* table, const std::shared_ptr<EntrySchema>& entry_schema,
    std::vector<CarquetExportColumn>& columns, carquet_error_t* error) {
  if (table == nullptr) {
    CARQUET_SET_ERROR(error, CARQUET_ERROR_INVALID_ARGUMENT, "Table is null");
    return nullptr;
  }

  carquet_schema_t* schema = carquet_schema_create(error);
  if (schema == nullptr) {
    return nullptr;
  }

  columns.clear();
  columns.resize(static_cast<size_t>(table->arrays_size()));

  for (int i = 0; i < table->arrays_size(); ++i) {
    const std::string column_name =
        columnNameFromResponse(table, entry_schema, i);
    carquet_status_t status = CARQUET_ERROR_INVALID_ARGUMENT;
    if (entry_schema &&
        static_cast<size_t>(i) < entry_schema->columnTypes.size() &&
        entry_schema->columnTypes[i] &&
        entry_schema->columnTypes[i]->item_case() == ::common::DataType::kMap) {
      status = addMapSchemaFromDeclaredType(
          schema, column_name.c_str(), *entry_schema->columnTypes[i], 0,
          &columns[static_cast<size_t>(i)]);
    } else {
      status = addSchemaFromProtoArray(
          schema, column_name.c_str(), table->arrays(i), 0,
          &columns[static_cast<size_t>(i)]);
    }
    if (status != CARQUET_OK) {
      carquet_schema_free(schema);
      CARQUET_SET_ERROR(error, status, "Failed to add column to Carquet schema");
      return nullptr;
    }
  }

  return schema;
}

}  // namespace reader
}  // namespace neug
