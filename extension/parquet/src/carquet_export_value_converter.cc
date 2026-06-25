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

#include "parquet/carquet_export_value_converter.h"

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/write/writer.h"

namespace neug {
namespace reader {
namespace {

using writer::StringFormatBuffer;

bool hasNulls(const std::string& validity, int64_t num_rows) {
  if (validity.empty()) {
    return false;
  }
  for (int64_t i = 0; i < num_rows; ++i) {
    if (!StringFormatBuffer::validateProtoValue(validity, static_cast<int>(i))) {
      return true;
    }
  }
  return false;
}

void fillCarquetByteArrays(const std::vector<std::string>& storage,
                           std::vector<carquet_byte_array_t>* byte_arrays) {
  byte_arrays->clear();
  byte_arrays->reserve(storage.size());
  for (const auto& str : storage) {
    byte_arrays->push_back(carquet_byte_array_t{
        reinterpret_cast<uint8_t*>(const_cast<char*>(str.data())),
        static_cast<int32_t>(str.size())});
  }
}

neug::Status writeCarquetBatch(carquet_writer_t* writer, int32_t column_index,
                               const void* values, int64_t num_values,
                               const int16_t* def_levels,
                               const int16_t* rep_levels) {
  const auto status = carquet_writer_write_batch(
      writer, column_index, values, num_values, def_levels, rep_levels);
  if (status != CARQUET_OK) {
    return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                        "Carquet write failed for column " +
                            std::to_string(column_index));
  }
  return neug::Status::OK();
}

template <typename T>
neug::Status writePrimitiveColumn(carquet_writer_t* writer,
                                  const carquet_schema_t* schema,
                                  int32_t column_index, const T* values,
                                  const std::string& validity,
                                  int64_t num_rows) {
  const int16_t max_def =
      carquet_schema_max_def_level(schema, column_index);
  if (max_def == 0 || !hasNulls(validity, num_rows)) {
    return writeCarquetBatch(writer, column_index, values, num_rows, nullptr,
                             nullptr);
  }

  std::vector<T> packed;
  std::vector<int16_t> def_levels;
  packed.reserve(static_cast<size_t>(num_rows));
  def_levels.reserve(static_cast<size_t>(num_rows));
  for (int64_t i = 0; i < num_rows; ++i) {
    if (StringFormatBuffer::validateProtoValue(validity, static_cast<int>(i))) {
      def_levels.push_back(max_def);
      packed.push_back(values[static_cast<size_t>(i)]);
    } else {
      def_levels.push_back(0);
    }
  }
  return writeCarquetBatch(writer, column_index, packed.data(), num_rows,
                           def_levels.data(), nullptr);
}

neug::Status writeStringColumn(carquet_writer_t* writer,
                               const carquet_schema_t* schema,
                               int32_t column_index,
                               const google::protobuf::RepeatedPtrField<
                                   std::string>& values,
                               const std::string& validity, int64_t num_rows) {
  const int16_t max_def =
      carquet_schema_max_def_level(schema, column_index);
  std::vector<std::string> storage;
  std::vector<carquet_byte_array_t> byte_arrays;
  std::vector<int16_t> def_levels;

  storage.reserve(static_cast<size_t>(num_rows));
  byte_arrays.reserve(static_cast<size_t>(num_rows));
  if (max_def > 0) {
    def_levels.reserve(static_cast<size_t>(num_rows));
  }

  for (int64_t i = 0; i < num_rows; ++i) {
    if (max_def > 0 &&
        !StringFormatBuffer::validateProtoValue(validity, static_cast<int>(i))) {
      def_levels.push_back(0);
      continue;
    }
    storage.push_back(values.Get(static_cast<int>(i)));
    if (max_def > 0) {
      def_levels.push_back(max_def);
    }
  }

  fillCarquetByteArrays(storage, &byte_arrays);

  return writeCarquetBatch(
      writer, column_index, byte_arrays.data(),
      max_def > 0 ? num_rows : static_cast<int64_t>(byte_arrays.size()),
      max_def > 0 ? def_levels.data() : nullptr, nullptr);
}

neug::Status writeDateColumn(carquet_writer_t* writer,
                             const carquet_schema_t* schema,
                             int32_t column_index,
                             const google::protobuf::RepeatedField<int64_t>& values,
                             const std::string& validity, int64_t num_rows) {
  std::vector<int32_t> days;
  days.reserve(static_cast<size_t>(num_rows));
  for (int64_t i = 0; i < num_rows; ++i) {
    // Use floor division: C++ integer division truncates toward zero,
    // but date conversion requires truncation toward negative infinity.
    const int64_t ms = values.Get(static_cast<int>(i));
    int64_t day = ms / 86400000LL;
    if (ms < 0 && ms % 86400000LL != 0) {
      --day;
    }
    days.push_back(static_cast<int32_t>(day));
  }
  return writePrimitiveColumn(writer, schema, column_index, days.data(),
                              validity, num_rows);
}

neug::Status writeListColumn(carquet_writer_t* writer,
                             const carquet_schema_t* schema,
                             const CarquetExportColumn& column,
                             const Array& proto_array, int64_t num_rows) {
  const auto& list_arr = proto_array.list_array();
  const int32_t leaf_index = column.leaf_index;
  const int16_t max_def = carquet_schema_max_def_level(schema, leaf_index);
  const int16_t max_rep = carquet_schema_max_rep_level(schema, leaf_index);

  if (!list_arr.has_elements()) {
    std::vector<int16_t> def_levels(static_cast<size_t>(num_rows), 0);
    return writeCarquetBatch(writer, leaf_index, nullptr, num_rows,
                             def_levels.data(), def_levels.data());
  }

  const Array& elements = list_arr.elements();
  const auto& list_validity = list_arr.validity();

  // Generic helper: builds def_levels, rep_levels, and packed non-null values
  // from any protobuf repeated field with .Get(idx) and a validity bitmap.
  auto buildPackedList = [&](const auto& element_values,
                             const auto& element_validity, auto& packed,
                             std::vector<int16_t>& def_levels,
                             std::vector<int16_t>& rep_levels) {
    for (int64_t row = 0; row < num_rows; ++row) {
      const int32_t start = list_arr.offsets(static_cast<int>(row));
      const int32_t end = list_arr.offsets(static_cast<int>(row + 1));
      if (!StringFormatBuffer::validateProtoValue(list_validity,
                                                  static_cast<int>(row))) {
        def_levels.push_back(0);
        rep_levels.push_back(0);
        continue;
      }
      for (int32_t idx = start; idx < end; ++idx) {
        def_levels.push_back(max_def);
        rep_levels.push_back(idx == start ? 0 : max_rep);
        if (!StringFormatBuffer::validateProtoValue(element_validity, idx)) {
          def_levels.back() = static_cast<int16_t>(max_def - 1);
          continue;
        }
        packed.push_back(element_values.Get(idx));
      }
    }
  };

  if (elements.has_string_array()) {
    std::vector<std::string> storage;
    std::vector<carquet_byte_array_t> byte_arrays;
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    buildPackedList(elements.string_array().values(),
                    elements.string_array().validity(), storage, def_levels,
                    rep_levels);
    fillCarquetByteArrays(storage, &byte_arrays);
    return writeCarquetBatch(writer, leaf_index, byte_arrays.data(),
                             static_cast<int64_t>(def_levels.size()),
                             def_levels.data(), rep_levels.data());
  }

  if (elements.has_int32_array()) {
    std::vector<int32_t> packed;
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    buildPackedList(elements.int32_array().values(),
                    elements.int32_array().validity(), packed, def_levels,
                    rep_levels);
    return writeCarquetBatch(writer, leaf_index, packed.data(),
                             static_cast<int64_t>(def_levels.size()),
                             def_levels.data(), rep_levels.data());
  }

  if (elements.has_int64_array()) {
    std::vector<int64_t> packed;
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    buildPackedList(elements.int64_array().values(),
                    elements.int64_array().validity(), packed, def_levels,
                    rep_levels);
    return writeCarquetBatch(writer, leaf_index, packed.data(),
                             static_cast<int64_t>(def_levels.size()),
                             def_levels.data(), rep_levels.data());
  }

  return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                      "Unsupported list element type for Carquet export");
}

neug::Status writeMapColumn(carquet_writer_t* writer,
                            const carquet_schema_t* schema,
                            const CarquetExportColumn& column,
                            const Array& proto_array, int64_t num_rows) {
  if (column.map_key_leaf < 0 || column.map_value_leaf < 0) {
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Incomplete Carquet MAP export column metadata");
  }
  if (!proto_array.has_list_array()) {
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "MAP export expects list_array payload");
  }

  const auto& list_arr = proto_array.list_array();
  const int16_t key_max_def =
      carquet_schema_max_def_level(schema, column.map_key_leaf);
  const int16_t val_max_def =
      carquet_schema_max_def_level(schema, column.map_value_leaf);
  const int16_t max_rep =
      carquet_schema_max_rep_level(schema, column.map_key_leaf);

  if (!list_arr.has_elements() ||
      !list_arr.elements().has_struct_array() ||
      list_arr.elements().struct_array().fields_size() < 2) {
    std::vector<int16_t> empty_def(static_cast<size_t>(num_rows), 0);
    const auto key_status = writeCarquetBatch(
        writer, column.map_key_leaf, nullptr, num_rows, empty_def.data(),
        empty_def.data());
    if (!key_status.ok()) {
      return key_status;
    }
    return writeCarquetBatch(writer, column.map_value_leaf, nullptr, num_rows,
                             empty_def.data(), empty_def.data());
  }

  const auto& struct_arr = list_arr.elements().struct_array();
  const auto& key_field = struct_arr.fields(0);
  const auto& val_field = struct_arr.fields(1);
  if (!key_field.has_string_array() || !val_field.has_string_array()) {
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Carquet MAP export currently supports string keys/values");
  }

  const auto& key_values = key_field.string_array().values();
  const auto& key_validity = key_field.string_array().validity();
  const auto& val_values = val_field.string_array().values();
  const auto& val_validity = val_field.string_array().validity();
  const auto& list_validity = list_arr.validity();

  std::vector<std::string> key_storage;
  std::vector<std::string> val_storage;
  std::vector<carquet_byte_array_t> key_bytes;
  std::vector<carquet_byte_array_t> val_bytes;
  std::vector<int16_t> key_def;
  std::vector<int16_t> key_rep;
  std::vector<int16_t> val_def;
  std::vector<int16_t> val_rep;

  for (int64_t row = 0; row < num_rows; ++row) {
    const int32_t start = list_arr.offsets(static_cast<int>(row));
    const int32_t end = list_arr.offsets(static_cast<int>(row + 1));
    if (!StringFormatBuffer::validateProtoValue(list_validity,
                                                static_cast<int>(row))) {
      key_def.push_back(0);
      key_rep.push_back(0);
      val_def.push_back(0);
      val_rep.push_back(0);
      continue;
    }
    for (int32_t idx = start; idx < end; ++idx) {
      const bool key_ok =
          StringFormatBuffer::validateProtoValue(key_validity, idx);
      const bool val_ok =
          StringFormatBuffer::validateProtoValue(val_validity, idx);
      key_def.push_back(key_ok ? key_max_def : static_cast<int16_t>(key_max_def - 1));
      key_rep.push_back(idx == start ? 0 : max_rep);
      val_def.push_back((key_ok && val_ok) ? val_max_def
                                            : static_cast<int16_t>(val_max_def - 1));
      val_rep.push_back(idx == start ? 0 : max_rep);
      if (key_ok) {
        key_storage.push_back(key_values.Get(idx));
      }
      if (key_ok && val_ok) {
        val_storage.push_back(val_values.Get(idx));
      }
    }
  }

  fillCarquetByteArrays(key_storage, &key_bytes);
  fillCarquetByteArrays(val_storage, &val_bytes);

  const auto key_status = writeCarquetBatch(
      writer, column.map_key_leaf, key_bytes.data(),
      static_cast<int64_t>(key_def.size()), key_def.data(), key_rep.data());
  if (!key_status.ok()) {
    return key_status;
  }
  return writeCarquetBatch(writer, column.map_value_leaf, val_bytes.data(),
                           static_cast<int64_t>(val_def.size()), val_def.data(),
                           val_rep.data());
}

neug::Status writeStructColumn(carquet_writer_t* writer,
                               const carquet_schema_t* schema,
                               const CarquetExportColumn& column,
                               const Array& proto_array, int64_t num_rows) {
  const auto& struct_arr = proto_array.struct_array();
  if (static_cast<int>(column.struct_leaf_indices.size()) !=
      struct_arr.fields_size()) {
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Struct field count mismatch");
  }

  for (int field_idx = 0; field_idx < struct_arr.fields_size(); ++field_idx) {
    const int32_t leaf_index = column.struct_leaf_indices[static_cast<size_t>(field_idx)];
    const auto status = writeCarquetColumn(
        writer, schema,
        CarquetExportColumn{.leaf_index = leaf_index,
                            .is_list = false,
                            .is_struct = false},
        struct_arr.fields(field_idx), num_rows);
    if (!status.ok()) {
      return status;
    }
  }
  return neug::Status::OK();
}

}  // namespace

neug::Status writeCarquetColumn(carquet_writer_t* writer,
                                const carquet_schema_t* schema,
                                const CarquetExportColumn& column,
                                const Array& proto_array, int64_t num_rows) {
  if (column.is_map) {
    return writeMapColumn(writer, schema, column, proto_array, num_rows);
  }
  if (column.is_list || proto_array.has_list_array()) {
    return writeListColumn(writer, schema, column, proto_array, num_rows);
  }
  if (column.is_struct || proto_array.has_struct_array()) {
    return writeStructColumn(writer, schema, column, proto_array, num_rows);
  }

  const int32_t leaf_index = column.leaf_index;
  if (proto_array.has_int32_array()) {
    const auto& arr = proto_array.int32_array();
    return writePrimitiveColumn(writer, schema, leaf_index, arr.values().data(),
                                arr.validity(), num_rows);
  }
  if (proto_array.has_int64_array()) {
    const auto& arr = proto_array.int64_array();
    return writePrimitiveColumn(writer, schema, leaf_index, arr.values().data(),
                                arr.validity(), num_rows);
  }
  if (proto_array.has_uint32_array()) {
    const auto& arr = proto_array.uint32_array();
    return writePrimitiveColumn(writer, schema, leaf_index, arr.values().data(),
                                arr.validity(), num_rows);
  }
  if (proto_array.has_uint64_array()) {
    const auto& arr = proto_array.uint64_array();
    return writePrimitiveColumn(writer, schema, leaf_index, arr.values().data(),
                                arr.validity(), num_rows);
  }
  if (proto_array.has_float_array()) {
    const auto& arr = proto_array.float_array();
    return writePrimitiveColumn(writer, schema, leaf_index, arr.values().data(),
                                arr.validity(), num_rows);
  }
  if (proto_array.has_double_array()) {
    const auto& arr = proto_array.double_array();
    return writePrimitiveColumn(writer, schema, leaf_index, arr.values().data(),
                                arr.validity(), num_rows);
  }
  if (proto_array.has_bool_array()) {
    const auto& arr = proto_array.bool_array();
    std::vector<uint8_t> bool_values(static_cast<size_t>(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
      bool_values[static_cast<size_t>(i)] =
          arr.values(static_cast<int>(i)) ? 1 : 0;
    }
    return writePrimitiveColumn(writer, schema, leaf_index, bool_values.data(),
                                arr.validity(), num_rows);
  }
  if (proto_array.has_string_array()) {
    const auto& arr = proto_array.string_array();
    return writeStringColumn(writer, schema, leaf_index, arr.values(),
                             arr.validity(), num_rows);
  }
  if (proto_array.has_date_array()) {
    const auto& arr = proto_array.date_array();
    return writeDateColumn(writer, schema, leaf_index, arr.values(),
                           arr.validity(), num_rows);
  }
  if (proto_array.has_timestamp_array()) {
    const auto& arr = proto_array.timestamp_array();
    return writePrimitiveColumn(writer, schema, leaf_index, arr.values().data(),
                                arr.validity(), num_rows);
  }
  if (proto_array.has_interval_array()) {
    const auto& arr = proto_array.interval_array();
    return writeStringColumn(writer, schema, leaf_index, arr.values(),
                             arr.validity(), num_rows);
  }
  if (proto_array.has_vertex_array()) {
    const auto& arr = proto_array.vertex_array();
    return writeStringColumn(writer, schema, leaf_index, arr.values(),
                             arr.validity(), num_rows);
  }
  if (proto_array.has_edge_array()) {
    const auto& arr = proto_array.edge_array();
    return writeStringColumn(writer, schema, leaf_index, arr.values(),
                             arr.validity(), num_rows);
  }
  if (proto_array.has_path_array()) {
    const auto& arr = proto_array.path_array();
    return writeStringColumn(writer, schema, leaf_index, arr.values(),
                             arr.validity(), num_rows);
  }

  return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                      "Unsupported protobuf array type for Carquet export");
}

}  // namespace reader
}  // namespace neug
