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

#include "parquet/carquet_value_converter.h"

#include <cstring>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/utils/exception/exception.h"
#include "parquet/carquet_type_converter.h"

namespace neug {
namespace reader {
namespace {

constexpr const char* kFilePrefix = "file://";

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

bool isLocalPath(const std::string& path) {
  const auto pos = path.find("://");
  return pos == std::string::npos || path.rfind(kFilePrefix, 0) == 0;
}

void throwCarquetError(const carquet_error_t& err, const std::string& ctx) {
  char buf[512];
  carquet_error_format(&err, buf, sizeof(buf));
  THROW_IO_EXCEPTION(ctx + ": " + buf);
}

bool isNullAt(const uint8_t* null_bitmap, int64_t index) {
  if (null_bitmap == nullptr) {
    return false;
  }
  return (null_bitmap[index / 8] & static_cast<uint8_t>(1 << (index % 8))) ==
         0;
}

template <typename T, typename ArrowLikeT = T>
std::shared_ptr<execution::IContextColumn> buildNumericColumn(
    const T* data, const uint8_t* null_bitmap, int64_t num_values) {
  execution::ValueColumnBuilder<ArrowLikeT> builder(null_bitmap != nullptr);
  builder.reserve(static_cast<size_t>(num_values));
  for (int64_t i = 0; i < num_values; ++i) {
    if (isNullAt(null_bitmap, i)) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(static_cast<ArrowLikeT>(data[i]));
    }
  }
  return builder.finish();
}

std::shared_ptr<execution::IContextColumn> buildStringColumn(
    const carquet_byte_array_t* data, const uint8_t* null_bitmap,
    int64_t num_values) {
  execution::ValueColumnBuilder<std::string> builder(null_bitmap != nullptr);
  builder.reserve(static_cast<size_t>(num_values));
  for (int64_t i = 0; i < num_values; ++i) {
    if (isNullAt(null_bitmap, i)) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(std::string(reinterpret_cast<const char*>(data[i].data),
                                        static_cast<size_t>(data[i].length)));
    }
  }
  return builder.finish();
}

std::shared_ptr<execution::IContextColumn> buildTimestampFromInt64(
    const int64_t* data, const uint8_t* null_bitmap, int64_t num_values,
    carquet_time_unit_t unit) {
  execution::ValueColumnBuilder<int64_t> builder(null_bitmap != nullptr);
  builder.reserve(static_cast<size_t>(num_values));
  for (int64_t i = 0; i < num_values; ++i) {
    if (isNullAt(null_bitmap, i)) {
      builder.push_back_null();
      continue;
    }
    int64_t millis = data[i];
    switch (unit) {
    case CARQUET_TIME_UNIT_MICROS:
      millis = data[i] / 1000;
      break;
    case CARQUET_TIME_UNIT_NANOS:
      millis = data[i] / 1000000;
      break;
    case CARQUET_TIME_UNIT_MILLIS:
    default:
      break;
    }
    builder.push_back_opt(millis);
  }
  return builder.finish();
}

}  // namespace

CarquetReaderHandle openCarquetReader(fsys::FileSystem& fs,
                                      const std::string& path,
                                      const ParquetReadOptions& options) {
  carquet_reader_options_t reader_opts;
  carquet_reader_options_init(&reader_opts);
  reader_opts.use_mmap = options.use_mmap;

  carquet_error_t err = CARQUET_ERROR_INIT;
  CarquetReaderHandle handle;

  if (isLocalPath(path)) {
    const std::string local_path = normalizeLocalPath(path);
    handle.reader =
        carquet_reader_open(local_path.c_str(), &reader_opts, &err);
    if (handle.reader == nullptr) {
      throwCarquetError(err, "Failed to open Parquet file: " + local_path);
    }
    return handle;
  }

  auto input = fs.openInputFile(path);
  if (!input) {
    THROW_IO_EXCEPTION("Failed to open Parquet file: " + path);
  }
  const int64_t size = input->Size();
  if (size < 0) {
    THROW_IO_EXCEPTION("Failed to determine Parquet file size: " + path);
  }
  handle.owned_buffer.resize(static_cast<size_t>(size));
  int64_t bytes_read = 0;
  auto status =
      input->Read(size, handle.owned_buffer.data(), &bytes_read);
  if (!status.ok() || bytes_read != size) {
    THROW_IO_EXCEPTION("Failed to read Parquet file into memory: " + path);
  }
  handle.reader = carquet_reader_open_buffer(
      handle.owned_buffer.data(), static_cast<size_t>(bytes_read), &reader_opts,
      &err);
  if (handle.reader == nullptr) {
    throwCarquetError(err, "Failed to open in-memory Parquet: " + path);
  }
  return handle;
}

void closeCarquetReader(CarquetReaderHandle& handle) {
  if (handle.reader != nullptr) {
    carquet_reader_close(handle.reader);
    handle.reader = nullptr;
  }
  handle.owned_buffer.clear();
}

std::shared_ptr<execution::IContextColumn> carquetBatchColumnToValueColumn(
    const carquet_row_batch_t* batch, int32_t batch_column_index,
    carquet_physical_type_t physical,
    const carquet_logical_type_t* logical) {
  const void* data = nullptr;
  const uint8_t* null_bitmap = nullptr;
  int64_t num_values = 0;
  if (carquet_row_batch_column(batch, batch_column_index, &data, &null_bitmap,
                               &num_values) != CARQUET_OK) {
    THROW_IO_EXCEPTION("Failed to read Carquet batch column " +
                       std::to_string(batch_column_index));
  }

  if (logical != nullptr) {
    switch (logical->id) {
    case CARQUET_LOGICAL_STRING:
    case CARQUET_LOGICAL_JSON:
    case CARQUET_LOGICAL_ENUM:
    case CARQUET_LOGICAL_UUID:
      return buildStringColumn(static_cast<const carquet_byte_array_t*>(data),
                               null_bitmap, num_values);
    case CARQUET_LOGICAL_DATE:
      return buildNumericColumn(static_cast<const int32_t*>(data), null_bitmap,
                                num_values);
    case CARQUET_LOGICAL_TIMESTAMP:
      return buildTimestampFromInt64(static_cast<const int64_t*>(data),
                                     null_bitmap, num_values,
                                     logical->params.timestamp.unit);
    case CARQUET_LOGICAL_INTEGER:
      if (logical->params.integer.bit_width <= 32) {
        if (logical->params.integer.is_signed) {
          return buildNumericColumn(static_cast<const int32_t*>(data),
                                    null_bitmap, num_values);
        }
        return buildNumericColumn(static_cast<const uint32_t*>(data),
                                  null_bitmap, num_values);
      }
      if (logical->params.integer.is_signed) {
        return buildNumericColumn(static_cast<const int64_t*>(data), null_bitmap,
                                  num_values);
      }
      return buildNumericColumn(static_cast<const uint64_t*>(data), null_bitmap,
                                num_values);
    default:
      break;
    }
  }

  switch (physical) {
  case CARQUET_PHYSICAL_BOOLEAN: {
    execution::ValueColumnBuilder<bool> builder(null_bitmap != nullptr);
    builder.reserve(static_cast<size_t>(num_values));
    const auto* bool_data = static_cast<const uint8_t*>(data);
    for (int64_t i = 0; i < num_values; ++i) {
      if (isNullAt(null_bitmap, i)) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(bool_data[i] != 0);
      }
    }
    return builder.finish();
  }
  case CARQUET_PHYSICAL_INT32:
    return buildNumericColumn(static_cast<const int32_t*>(data), null_bitmap,
                              num_values);
  case CARQUET_PHYSICAL_INT64:
    return buildNumericColumn(static_cast<const int64_t*>(data), null_bitmap,
                              num_values);
  case CARQUET_PHYSICAL_FLOAT:
    return buildNumericColumn(static_cast<const float*>(data), null_bitmap,
                              num_values);
  case CARQUET_PHYSICAL_DOUBLE:
    return buildNumericColumn(static_cast<const double*>(data), null_bitmap,
                              num_values);
  case CARQUET_PHYSICAL_BYTE_ARRAY:
    return buildStringColumn(static_cast<const carquet_byte_array_t*>(data),
                             null_bitmap, num_values);
  case CARQUET_PHYSICAL_INT96:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Carquet INT96 columns are not supported; rewrite file with TIMESTAMP");
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported Carquet column physical type: " +
        std::string(carquet_physical_type_name(physical)));
  }
}

execution::DataChunk carquetBatchToDataChunk(
    const carquet_row_batch_t* batch,
    const std::vector<carquet_physical_type_t>& physical_types,
    const std::vector<const carquet_logical_type_t*>& logical_types) {
  execution::DataChunk chunk;
  const int32_t num_columns = carquet_row_batch_num_columns(batch);
  for (int32_t i = 0; i < num_columns; ++i) {
    const carquet_physical_type_t physical =
        i < static_cast<int32_t>(physical_types.size())
            ? physical_types[static_cast<size_t>(i)]
            : CARQUET_PHYSICAL_BYTE_ARRAY;
    const carquet_logical_type_t* logical =
        i < static_cast<int32_t>(logical_types.size())
            ? logical_types[static_cast<size_t>(i)]
            : nullptr;
    chunk.set(i, carquetBatchColumnToValueColumn(batch, i, physical, logical));
  }
  return chunk;
}

}  // namespace reader
}  // namespace neug
