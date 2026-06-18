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

#include "parquet/arrow_parquet_encoder.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include <parquet/arrow/writer.h>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/write/writer.h"
#include "parquet/arrow_fs_resolver.h"

namespace neug {
namespace reader {
namespace {

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

std::shared_ptr<arrow::DataType> inferArrowTypeFromArray(const Array& proto_array) {
  if (proto_array.has_int32_array()) {
    return arrow::int32();
  }
  if (proto_array.has_int64_array()) {
    return arrow::int64();
  }
  if (proto_array.has_uint32_array()) {
    return arrow::uint32();
  }
  if (proto_array.has_uint64_array()) {
    return arrow::uint64();
  }
  if (proto_array.has_float_array()) {
    return arrow::float32();
  }
  if (proto_array.has_double_array()) {
    return arrow::float64();
  }
  if (proto_array.has_bool_array()) {
    return arrow::boolean();
  }
  if (proto_array.has_string_array()) {
    return arrow::large_utf8();
  }
  if (proto_array.has_date_array()) {
    return arrow::date64();
  }
  if (proto_array.has_timestamp_array()) {
    return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
  }
  if (proto_array.has_list_array()) {
    const auto& list_arr = proto_array.list_array();
    if (list_arr.has_elements()) {
      return arrow::list(inferArrowTypeFromArray(list_arr.elements()));
    }
    return arrow::list(arrow::large_utf8());
  }
  if (proto_array.has_struct_array()) {
    const auto& struct_arr = proto_array.struct_array();
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int i = 0; i < struct_arr.fields_size(); ++i) {
      fields.push_back(arrow::field(
          "field_" + std::to_string(i),
          inferArrowTypeFromArray(struct_arr.fields(i))));
    }
    return arrow::struct_(fields);
  }
  if (proto_array.has_vertex_array() || proto_array.has_edge_array() ||
      proto_array.has_path_array() || proto_array.has_interval_array()) {
    return arrow::large_utf8();
  }
  LOG(WARNING) << "Unknown protobuf array type, defaulting to large_utf8";
  return arrow::large_utf8();
}

#define TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(PROTO_FIELD, BUILDER_TYPE, VALUES_FIELD) \
  { \
    auto& arr = proto_array.PROTO_FIELD(); \
    BUILDER_TYPE builder(pool); \
    for (int i = 0; i < arr.values_size(); ++i) { \
      if (writer::StringFormatBuffer::validateProtoValue(arr.validity(), i)) { \
        auto status = builder.Append(arr.VALUES_FIELD(i)); \
        if (!status.ok()) { \
          THROW_RUNTIME_ERROR("Failed to append value: " + status.ToString()); \
        } \
      } else { \
        auto status = builder.AppendNull(); \
        if (!status.ok()) { \
          THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString()); \
        } \
      } \
    } \
    std::shared_ptr<arrow::Array> result; \
    auto status = builder.Finish(&result); \
    if (!status.ok()) { \
      THROW_RUNTIME_ERROR("Failed to finish array: " + status.ToString()); \
    } \
    return result; \
  }

std::shared_ptr<arrow::Array> protoArrayToArrowArray(
    const Array& proto_array, const std::shared_ptr<arrow::DataType>& arrow_type,
    int row_count) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  if (proto_array.has_int32_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(int32_array, arrow::Int32Builder, values)
  }
  if (proto_array.has_int64_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(int64_array, arrow::Int64Builder, values)
  }
  if (proto_array.has_uint32_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(uint32_array, arrow::UInt32Builder, values)
  }
  if (proto_array.has_uint64_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(uint64_array, arrow::UInt64Builder, values)
  }
  if (proto_array.has_float_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(float_array, arrow::FloatBuilder, values)
  }
  if (proto_array.has_double_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(double_array, arrow::DoubleBuilder, values)
  }
  if (proto_array.has_bool_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(bool_array, arrow::BooleanBuilder, values)
  }
  if (proto_array.has_string_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(string_array, arrow::LargeStringBuilder, values)
  }
  if (proto_array.has_date_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(date_array, arrow::Date64Builder, values)
  }
  if (proto_array.has_interval_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(interval_array, arrow::LargeStringBuilder, values)
  }
  if (proto_array.has_timestamp_array()) {
    auto& arr = proto_array.timestamp_array();
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                                    pool);
    for (int i = 0; i < arr.values_size(); ++i) {
      if (writer::StringFormatBuffer::validateProtoValue(arr.validity(), i)) {
        auto status = builder.Append(arr.values(i));
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append timestamp value: " + status.ToString());
        }
      } else {
        auto status = builder.AppendNull();
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish timestamp array: " + status.ToString());
    }
    return result;
  }
  if (proto_array.has_list_array()) {
    const auto& list_arr = proto_array.list_array();
    auto list_type = std::static_pointer_cast<arrow::ListType>(arrow_type);
    auto elements_array =
        protoArrayToArrowArray(list_arr.elements(), list_type->value_type(), 0);
    int64_t num_rows = list_arr.offsets_size() - 1;
    int64_t offsets_byte_size = list_arr.offsets_size() * sizeof(int32_t);
    auto offsets_buffer_result = arrow::AllocateBuffer(offsets_byte_size);
    if (!offsets_buffer_result.ok()) {
      THROW_RUNTIME_ERROR("Failed to allocate offsets buffer: " +
                          offsets_buffer_result.status().ToString());
    }
    std::shared_ptr<arrow::Buffer> offsets_buffer =
        std::move(offsets_buffer_result.ValueOrDie());
    memcpy(offsets_buffer->mutable_data(), list_arr.offsets().data(),
           offsets_byte_size);
    return std::make_shared<arrow::ListArray>(arrow_type, num_rows, offsets_buffer,
                                              elements_array, nullptr);
  }
  if (proto_array.has_struct_array()) {
    const auto& struct_arr = proto_array.struct_array();
    auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow_type);
    std::vector<std::shared_ptr<arrow::Array>> field_arrays;
    for (int i = 0; i < struct_arr.fields_size(); ++i) {
      field_arrays.push_back(protoArrayToArrowArray(
          struct_arr.fields(i), struct_type->field(i)->type(), row_count));
    }
    std::shared_ptr<arrow::Buffer> validity_buffer;
    const auto& null_bitmap = struct_arr.validity();
    if (!null_bitmap.empty()) {
      auto buffer_result = arrow::AllocateBuffer(null_bitmap.size());
      if (!buffer_result.ok()) {
        THROW_RUNTIME_ERROR("Failed to allocate validity buffer: " +
                            buffer_result.status().ToString());
      }
      validity_buffer = std::move(buffer_result.ValueOrDie());
      memcpy(validity_buffer->mutable_data(), null_bitmap.data(),
             null_bitmap.size());
    }
    int64_t num_rows = field_arrays.empty() ? 0 : field_arrays[0]->length();
    return std::make_shared<arrow::StructArray>(struct_type, num_rows, field_arrays,
                                                validity_buffer);
  }
  if (proto_array.has_vertex_array() || proto_array.has_edge_array() ||
      proto_array.has_path_array()) {
    arrow::LargeStringBuilder builder(pool);
    auto append_json_strings = [&](const auto& arr) {
      for (int i = 0; i < arr.values_size(); ++i) {
        if (writer::StringFormatBuffer::validateProtoValue(arr.validity(), i)) {
          auto status = builder.Append(arr.values(i));
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append JSON string: " + status.ToString());
          }
        } else {
          auto status = builder.AppendNull();
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
          }
        }
      }
    };
    if (proto_array.has_vertex_array()) {
      append_json_strings(proto_array.vertex_array());
    } else if (proto_array.has_edge_array()) {
      append_json_strings(proto_array.edge_array());
    } else {
      append_json_strings(proto_array.path_array());
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish JSON string array: " + status.ToString());
    }
    return result;
  }

  THROW_INVALID_ARGUMENT_EXCEPTION(
      "Unsupported protobuf array type for conversion");
}

}  // namespace

neug::Status ArrowParquetEncoder::writeTable(
    fsys::FileSystem& fs, const std::string& path, const QueryResponse* table,
    const std::shared_ptr<EntrySchema>& entry_schema,
    const ParquetWriteOptions& options) {
  if (!table || table->row_count() == 0) {
    return neug::Status::OK();
  }

  try {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    const int num_columns = table->arrays_size();
    for (int i = 0; i < num_columns; ++i) {
      fields.push_back(arrow::field(
          columnNameFromResponse(table, entry_schema, i),
          inferArrowTypeFromArray(table->arrays(i))));
    }
    auto arrow_schema = arrow::schema(fields);

    auto arrow_fs = parquet_vfs::resolveArrowFileSystem(fs);
    auto result = arrow_fs->OpenOutputStream(path);
    if (!result.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to open output file: " + result.status().ToString());
    }
    auto outfile = result.ValueOrDie();

    if (!options.writer_properties) {
      return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Arrow writer properties are not initialized");
    }

    auto writer_result = ::parquet::arrow::FileWriter::Open(
        *arrow_schema, arrow::default_memory_pool(), outfile,
        options.writer_properties);
    if (!writer_result.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to create Parquet writer: " +
                              writer_result.status().ToString());
    }
    auto writer = std::move(writer_result.ValueOrDie());

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(static_cast<size_t>(num_columns));
    for (int i = 0; i < num_columns; ++i) {
      arrays.push_back(protoArrayToArrowArray(
          table->arrays(i), arrow_schema->field(i)->type(), table->row_count()));
    }

    auto arrow_table = arrow::Table::Make(arrow_schema, arrays);
    auto write_status = writer->WriteTable(*arrow_table, arrow_table->num_rows());
    if (!write_status.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to write Parquet table: " + write_status.ToString());
    }

    auto close_status = writer->Close();
    if (!close_status.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to close Parquet writer: " + close_status.ToString());
    }

    auto outfile_close_status = outfile->Close();
    if (!outfile_close_status.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to close output stream: " +
                              outfile_close_status.ToString());
    }

    return neug::Status::OK();
  } catch (const std::exception& e) {
    return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                        std::string("Failed to write Parquet table: ") + e.what());
  }
}

}  // namespace reader
}  // namespace neug
