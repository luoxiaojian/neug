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

#include <algorithm>
#include <climits>
#include <cstring>

#include <glog/logging.h>

#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/struct_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/types/value.h"
#include "neug/common/types.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"
#include "parquet/carquet_type_converter.h"

namespace neug {
namespace reader {
namespace {

using execution::date_t;
using execution::timestamp_ms_t;
using execution::Value;

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

std::shared_ptr<execution::IContextColumn> buildDateColumnFromDays(
    const int32_t* data, const uint8_t* null_bitmap, int64_t num_values) {
  execution::ValueColumnBuilder<date_t> builder(null_bitmap != nullptr);
  builder.reserve(static_cast<size_t>(num_values));
  for (int64_t i = 0; i < num_values; ++i) {
    if (isNullAt(null_bitmap, i)) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(date_t(data[i]));
    }
  }
  return builder.finish();
}

int64_t carquetTimestampToMillis(int64_t value, carquet_time_unit_t unit) {
  switch (unit) {
  case CARQUET_TIME_UNIT_MICROS:
    return value / 1000;
  case CARQUET_TIME_UNIT_NANOS:
    return value / 1000000;
  case CARQUET_TIME_UNIT_MILLIS:
  default:
    return value;
  }
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
  execution::ValueColumnBuilder<timestamp_ms_t> builder(null_bitmap != nullptr);
  builder.reserve(static_cast<size_t>(num_values));
  for (int64_t i = 0; i < num_values; ++i) {
    if (isNullAt(null_bitmap, i)) {
      builder.push_back_null();
      continue;
    }
    builder.push_back_opt(
        timestamp_ms_t(carquetTimestampToMillis(data[i], unit)));
  }
  return builder.finish();
}

std::shared_ptr<execution::IContextColumn> buildInt96TimestampColumn(
    const carquet_int96_t* data, const uint8_t* null_bitmap, int64_t num_values) {
  execution::ValueColumnBuilder<timestamp_ms_t> builder(null_bitmap != nullptr);
  builder.reserve(static_cast<size_t>(num_values));
  for (int64_t i = 0; i < num_values; ++i) {
    if (isNullAt(null_bitmap, i)) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(
          timestamp_ms_t(int96ToMillis(data[static_cast<size_t>(i)])));
    }
  }
  return builder.finish();
}

DataType listElementDataType(carquet_physical_type_t physical,
                             const carquet_logical_type_t* logical) {
  const auto common_type = carquetColumnToCommonType(physical, logical);
  return parse_from_data_type(*common_type);
}

}  // namespace

int64_t int96ToMillis(const carquet_int96_t& value) {
  constexpr int64_t kJulianEpochOffsetDays = 2440588LL;
  constexpr int64_t kNanosPerMillis = 1000000LL;
  constexpr int64_t kMillisPerDay = 86400000LL;
  constexpr int64_t kNanosPerDay = 86400000000000LL;
  constexpr int64_t kMaxDays = INT64_MAX / kMillisPerDay;

  int64_t nanos_since_midnight = 0;
  std::memcpy(&nanos_since_midnight, value.value, sizeof(int64_t));

  // Clamp nanos_since_midnight to valid range [0, kNanosPerDay).
  if (nanos_since_midnight < 0) {
    nanos_since_midnight = 0;
  } else if (nanos_since_midnight >= kNanosPerDay) {
    nanos_since_midnight = kNanosPerDay - 1;
  }

  const int64_t days =
      static_cast<int64_t>(value.value[2]) - kJulianEpochOffsetDays;

  // Overflow guard: clamp to representable range.
  if (days > kMaxDays) {
    return INT64_MAX;
  }
  if (days < -kMaxDays) {
    return INT64_MIN;
  }
  return days * kMillisPerDay + nanos_since_midnight / kNanosPerMillis;
}

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

CarquetReaderHandle::CarquetReaderHandle(CarquetReaderHandle&& other) noexcept
    : reader(other.reader), owned_buffer(std::move(other.owned_buffer)) {
  other.reader = nullptr;
}

CarquetReaderHandle& CarquetReaderHandle::operator=(
    CarquetReaderHandle&& other) noexcept {
  if (this != &other) {
    close();
    reader = other.reader;
    owned_buffer = std::move(other.owned_buffer);
    other.reader = nullptr;
  }
  return *this;
}

void CarquetReaderHandle::close() {
  if (reader != nullptr) {
    carquet_reader_close(reader);
    reader = nullptr;
  }
  owned_buffer.clear();
  owned_buffer.shrink_to_fit();
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
      return buildDateColumnFromDays(static_cast<const int32_t*>(data),
                                     null_bitmap, num_values);
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
    return buildInt96TimestampColumn(static_cast<const carquet_int96_t*>(data),
                                     null_bitmap, num_values);
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
    if (i >= static_cast<int32_t>(physical_types.size())) {
      LOG(WARNING) << "Batch column " << i << " has no pre-parsed physical "
                   << "type; falling back to BYTE_ARRAY";
    }
    const carquet_logical_type_t* logical =
        i < static_cast<int32_t>(logical_types.size())
            ? logical_types[static_cast<size_t>(i)]
            : nullptr;
    chunk.set(i, carquetBatchColumnToValueColumn(batch, i, physical, logical));
  }
  return chunk;
}

namespace {

using execution::date_t;
using execution::timestamp_ms_t;
using execution::Value;

template <typename OutputT, typename InputT, typename ConvertFn>
void appendFlatConvertedValuesFromColumnReader(
    carquet_column_reader_t* col_reader,
    execution::ValueColumnBuilder<OutputT>& builder, int16_t max_def,
    ConvertFn convert) {
  std::vector<InputT> values(4096);
  std::vector<int16_t> def_levels(4096);
  while (true) {
    const int64_t count = carquet_column_read_batch(
        col_reader, values.data(), static_cast<int64_t>(values.size()),
        max_def > 0 ? def_levels.data() : nullptr, nullptr);
    if (count <= 0) {
      break;
    }
    if (max_def == 0) {
      for (int64_t i = 0; i < count; ++i) {
        builder.push_back_opt(convert(values[static_cast<size_t>(i)]));
      }
      continue;
    }
    for (int64_t i = 0; i < count; ++i) {
      if (def_levels[static_cast<size_t>(i)] == max_def) {
        builder.push_back_opt(convert(values[static_cast<size_t>(i)]));
      } else {
        builder.push_back_null();
      }
    }
  }
}

template <typename T>
void appendFlatValuesFromColumnReader(
    carquet_column_reader_t* col_reader, execution::ValueColumnBuilder<T>& builder,
    int16_t max_def, size_t value_size) {
  std::vector<T> values;
  std::vector<int16_t> def_levels;
  values.resize(4096);
  def_levels.resize(4096);
  while (true) {
    const int64_t count = carquet_column_read_batch(
        col_reader, values.data(), static_cast<int64_t>(values.size()),
        max_def > 0 ? def_levels.data() : nullptr, nullptr);
    if (count <= 0) {
      break;
    }
    if (max_def == 0) {
      for (int64_t i = 0; i < count; ++i) {
        builder.push_back_opt(values[static_cast<size_t>(i)]);
      }
      continue;
    }
    for (int64_t i = 0; i < count; ++i) {
      if (def_levels[static_cast<size_t>(i)] == max_def) {
        builder.push_back_opt(values[static_cast<size_t>(i)]);
      } else {
        builder.push_back_null();
      }
    }
  }
  (void)value_size;
}

// --- Specialized append functions for non-trivially-typed columns ---

void appendBooleanValuesFromColumnReader(
    carquet_column_reader_t* col_reader,
    execution::ValueColumnBuilder<bool>& builder, int16_t max_def) {
  std::vector<uint8_t> values(4096);
  std::vector<int16_t> def_levels(4096);
  while (true) {
    const int64_t count = carquet_column_read_batch(
        col_reader, values.data(), static_cast<int64_t>(values.size()),
        max_def > 0 ? def_levels.data() : nullptr, nullptr);
    if (count <= 0) {
      break;
    }
    for (int64_t i = 0; i < count; ++i) {
      if (max_def > 0 && def_levels[static_cast<size_t>(i)] != max_def) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(values[static_cast<size_t>(i)] != 0);
      }
    }
  }
}

void appendByteArrayValuesFromColumnReader(
    carquet_column_reader_t* col_reader,
    execution::ValueColumnBuilder<std::string>& builder, int16_t max_def) {
  std::vector<carquet_byte_array_t> values(4096);
  std::vector<int16_t> def_levels(4096);
  while (true) {
    const int64_t count = carquet_column_read_batch(
        col_reader, values.data(), static_cast<int64_t>(values.size()),
        max_def > 0 ? def_levels.data() : nullptr, nullptr);
    if (count <= 0) {
      break;
    }
    for (int64_t i = 0; i < count; ++i) {
      if (max_def > 0 && def_levels[static_cast<size_t>(i)] != max_def) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(std::string(
            reinterpret_cast<const char*>(values[static_cast<size_t>(i)].data),
            static_cast<size_t>(values[static_cast<size_t>(i)].length)));
      }
    }
  }
}

void appendInt96TimestampValuesFromColumnReader(
    carquet_column_reader_t* col_reader,
    execution::ValueColumnBuilder<timestamp_ms_t>& builder, int16_t max_def) {
  std::vector<carquet_int96_t> values(4096);
  std::vector<int16_t> def_levels(4096);
  while (true) {
    const int64_t count = carquet_column_read_batch(
        col_reader, values.data(), static_cast<int64_t>(values.size()),
        max_def > 0 ? def_levels.data() : nullptr, nullptr);
    if (count <= 0) {
      break;
    }
    for (int64_t i = 0; i < count; ++i) {
      if (max_def > 0 && def_levels[static_cast<size_t>(i)] != max_def) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(
            timestamp_ms_t(int96ToMillis(values[static_cast<size_t>(i)])));
      }
    }
  }
}

/// Reads a flat column across all row groups using a generic append function.
/// Eliminates repeated row-group iteration + column-reader lifecycle boilerplate.
template <typename BuilderT, typename AppendFn>
std::shared_ptr<execution::IContextColumn> readFlatColumnAllRowGroups(
    carquet_reader_t* reader, int32_t leaf_index, int16_t max_def,
    AppendFn append_fn) {
  BuilderT builder(max_def > 0);
  carquet_error_t err = CARQUET_ERROR_INIT;
  const int32_t num_row_groups = carquet_reader_num_row_groups(reader);
  for (int32_t rg = 0; rg < num_row_groups; ++rg) {
    carquet_column_reader_t* col =
        carquet_reader_get_column(reader, rg, leaf_index, &err);
    if (col == nullptr) {
      throwCarquetError(err, "Failed to open Carquet column reader");
    }
    append_fn(col, builder, max_def);
    carquet_column_reader_free(col);
  }
  return builder.finish();
}

void appendListRowsFromBatch(
    execution::ListColumnBuilder& list_builder,
    carquet_physical_type_t physical, const carquet_logical_type_t* logical,
    const void* values, const int16_t* def_levels, const int16_t* rep_levels,
    int64_t count, int16_t max_rep_level, int16_t max_def_level) {
  if (count <= 0) {
    return;
  }

  std::vector<int64_t> offsets(static_cast<size_t>(count) + 1, 0);
  const int64_t num_lists =
      carquet_list_offsets(rep_levels, count, max_rep_level, offsets.data(),
                           static_cast<int64_t>(offsets.size()));
  offsets[static_cast<size_t>(num_lists)] = count;

  const DataType elem_type = listElementDataType(physical, logical);

  auto extract_value = [&](int64_t value_idx) -> Value {
    switch (physical) {
    case CARQUET_PHYSICAL_INT32:
      return Value::INT32(static_cast<const int32_t*>(values)[value_idx]);
    case CARQUET_PHYSICAL_INT64:
      return Value::INT64(static_cast<const int64_t*>(values)[value_idx]);
    case CARQUET_PHYSICAL_FLOAT:
      return Value::FLOAT(static_cast<const float*>(values)[value_idx]);
    case CARQUET_PHYSICAL_DOUBLE:
      return Value::DOUBLE(static_cast<const double*>(values)[value_idx]);
    case CARQUET_PHYSICAL_BOOLEAN:
      return Value::BOOLEAN(
          static_cast<const uint8_t*>(values)[value_idx] != 0);
    case CARQUET_PHYSICAL_BYTE_ARRAY: {
      const auto& bytes =
          static_cast<const carquet_byte_array_t*>(values)[value_idx];
      return Value::STRING(std::string(reinterpret_cast<const char*>(bytes.data),
                                       static_cast<size_t>(bytes.length)));
    }
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Unsupported list element physical type: " +
          std::string(carquet_physical_type_name(physical)));
    }
  };

  // value_idx must accumulate across all lists because the values buffer
  // is a packed array of non-null elements across the entire batch.
  int64_t value_idx = 0;
  for (int64_t list_idx = 0; list_idx < num_lists; ++list_idx) {
    const int64_t start = offsets[static_cast<size_t>(list_idx)];
    const int64_t end = offsets[static_cast<size_t>(list_idx + 1)];
    if (def_levels[start] == 0) {
      list_builder.push_back_elem(Value::LIST(elem_type, {}));
      continue;
    }

    std::vector<Value> row_values;
    for (int64_t i = start; i < end; ++i) {
      if (def_levels[i] == max_def_level) {
        row_values.push_back(extract_value(value_idx++));
      }
    }
    list_builder.push_back_elem(Value::LIST(elem_type, std::move(row_values)));
  }
}

}  // namespace

std::shared_ptr<execution::IContextColumn> readCarquetFlatColumn(
    carquet_reader_t* reader, int32_t leaf_index,
    carquet_physical_type_t physical, const carquet_logical_type_t* logical) {
  const int16_t max_def = carquet_schema_max_def_level(
      carquet_reader_schema(reader), leaf_index);

  switch (physical) {
  case CARQUET_PHYSICAL_BOOLEAN:
    return readFlatColumnAllRowGroups<execution::ValueColumnBuilder<bool>>(
        reader, leaf_index, max_def, appendBooleanValuesFromColumnReader);

  case CARQUET_PHYSICAL_INT32: {
    if (logical != nullptr && logical->id == CARQUET_LOGICAL_DATE) {
      return readFlatColumnAllRowGroups<execution::ValueColumnBuilder<date_t>>(
          reader, leaf_index, max_def,
          [](carquet_column_reader_t* col, auto& builder, int16_t md) {
            appendFlatConvertedValuesFromColumnReader<date_t, int32_t>(
                col, builder, md, [](int32_t days) { return date_t(days); });
          });
    }
    if (logical != nullptr && logical->id == CARQUET_LOGICAL_INTEGER &&
        !logical->params.integer.is_signed) {
      return readFlatColumnAllRowGroups<execution::ValueColumnBuilder<uint32_t>>(
          reader, leaf_index, max_def,
          [](carquet_column_reader_t* col, auto& builder, int16_t md) {
            appendFlatConvertedValuesFromColumnReader<uint32_t, int32_t>(
                col, builder, md,
                [](int32_t v) { return static_cast<uint32_t>(v); });
          });
    }
    return readFlatColumnAllRowGroups<execution::ValueColumnBuilder<int32_t>>(
        reader, leaf_index, max_def,
        [](carquet_column_reader_t* col, auto& builder, int16_t md) {
          appendFlatValuesFromColumnReader(col, builder, md, sizeof(int32_t));
        });
  }
  case CARQUET_PHYSICAL_INT64: {
    if (logical != nullptr && logical->id == CARQUET_LOGICAL_TIMESTAMP) {
      const carquet_time_unit_t unit = logical->params.timestamp.unit;
      return readFlatColumnAllRowGroups<
          execution::ValueColumnBuilder<timestamp_ms_t>>(
          reader, leaf_index, max_def,
          [unit](carquet_column_reader_t* col, auto& builder, int16_t md) {
            appendFlatConvertedValuesFromColumnReader<timestamp_ms_t, int64_t>(
                col, builder, md, [unit](int64_t v) {
                  return timestamp_ms_t(carquetTimestampToMillis(v, unit));
                });
          });
    }
    if (logical != nullptr && logical->id == CARQUET_LOGICAL_INTEGER &&
        !logical->params.integer.is_signed) {
      return readFlatColumnAllRowGroups<execution::ValueColumnBuilder<uint64_t>>(
          reader, leaf_index, max_def,
          [](carquet_column_reader_t* col, auto& builder, int16_t md) {
            appendFlatConvertedValuesFromColumnReader<uint64_t, int64_t>(
                col, builder, md,
                [](int64_t v) { return static_cast<uint64_t>(v); });
          });
    }
    return readFlatColumnAllRowGroups<execution::ValueColumnBuilder<int64_t>>(
        reader, leaf_index, max_def,
        [](carquet_column_reader_t* col, auto& builder, int16_t md) {
          appendFlatValuesFromColumnReader(col, builder, md, sizeof(int64_t));
        });
  }
  case CARQUET_PHYSICAL_FLOAT:
    return readFlatColumnAllRowGroups<execution::ValueColumnBuilder<float>>(
        reader, leaf_index, max_def,
        [](carquet_column_reader_t* col, auto& builder, int16_t md) {
          appendFlatValuesFromColumnReader(col, builder, md, sizeof(float));
        });

  case CARQUET_PHYSICAL_DOUBLE:
    return readFlatColumnAllRowGroups<execution::ValueColumnBuilder<double>>(
        reader, leaf_index, max_def,
        [](carquet_column_reader_t* col, auto& builder, int16_t md) {
          appendFlatValuesFromColumnReader(col, builder, md, sizeof(double));
        });

  case CARQUET_PHYSICAL_BYTE_ARRAY:
    return readFlatColumnAllRowGroups<
        execution::ValueColumnBuilder<std::string>>(
        reader, leaf_index, max_def, appendByteArrayValuesFromColumnReader);

  case CARQUET_PHYSICAL_INT96:
    return readFlatColumnAllRowGroups<
        execution::ValueColumnBuilder<timestamp_ms_t>>(
        reader, leaf_index, max_def,
        appendInt96TimestampValuesFromColumnReader);

  default:
    break;
  }

  (void)logical;
  THROW_NOT_SUPPORTED_EXCEPTION(
      "Unsupported flat Carquet column physical type: " +
      std::string(carquet_physical_type_name(physical)));
}

std::shared_ptr<execution::IContextColumn> readCarquetListColumn(
    carquet_reader_t* reader, int32_t leaf_index,
    carquet_physical_type_t physical, const carquet_logical_type_t* logical,
    int16_t max_rep_level, int16_t max_def_level) {
  const DataType elem_type = listElementDataType(physical, logical);
  execution::ListColumnBuilder list_builder(elem_type);
  carquet_error_t err = CARQUET_ERROR_INIT;
  const int32_t num_row_groups = carquet_reader_num_row_groups(reader);

  const size_t type_size = [&]() -> size_t {
    switch (physical) {
    case CARQUET_PHYSICAL_BOOLEAN:
      return 1;
    case CARQUET_PHYSICAL_INT32:
    case CARQUET_PHYSICAL_FLOAT:
      return 4;
    case CARQUET_PHYSICAL_INT64:
    case CARQUET_PHYSICAL_DOUBLE:
      return 8;
    case CARQUET_PHYSICAL_BYTE_ARRAY:
      return sizeof(carquet_byte_array_t);
    default:
      return 0;
    }
  }();

  if (type_size == 0) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported list element physical type: " +
        std::string(carquet_physical_type_name(physical)));
  }

  std::vector<uint8_t> value_buffer;
  value_buffer.resize(4096 * type_size);
  std::vector<int16_t> def_levels(4096);
  std::vector<int16_t> rep_levels(4096);

  for (int32_t rg = 0; rg < num_row_groups; ++rg) {
    carquet_column_reader_t* col =
        carquet_reader_get_column(reader, rg, leaf_index, &err);
    if (col == nullptr) {
      throwCarquetError(err, "Failed to open Carquet list column reader");
    }
    while (true) {
      const int64_t count = carquet_column_read_batch(
          col, value_buffer.data(), static_cast<int64_t>(def_levels.size()),
          def_levels.data(), rep_levels.data());
      if (count <= 0) {
        break;
      }
      appendListRowsFromBatch(list_builder, physical, logical,
                              value_buffer.data(), def_levels.data(),
                              rep_levels.data(), count, max_rep_level,
                              max_def_level);
    }
    carquet_column_reader_free(col);
  }

  return list_builder.finish();
}

Value extractFlatParquetValue(carquet_physical_type_t physical,
                              const carquet_logical_type_t* logical,
                              const void* values, int64_t index) {
  if (logical != nullptr && logical->id == CARQUET_LOGICAL_DATE) {
    return Value::DATE(date_t(static_cast<const int32_t*>(values)[index]));
  }
  if (logical != nullptr && logical->id == CARQUET_LOGICAL_TIMESTAMP) {
    const int64_t raw = static_cast<const int64_t*>(values)[index];
    return Value::TIMESTAMPMS(
        timestamp_ms_t(carquetTimestampToMillis(raw, logical->params.timestamp.unit)));
  }
  if (logical != nullptr && logical->id == CARQUET_LOGICAL_INTEGER &&
      !logical->params.integer.is_signed &&
      logical->params.integer.bit_width <= 32) {
    return Value::UINT32(
        static_cast<uint32_t>(static_cast<const int32_t*>(values)[index]));
  }
  if (logical != nullptr && logical->id == CARQUET_LOGICAL_INTEGER &&
      !logical->params.integer.is_signed) {
    return Value::UINT64(
        static_cast<uint64_t>(static_cast<const int64_t*>(values)[index]));
  }

  switch (physical) {
  case CARQUET_PHYSICAL_INT32:
    return Value::INT32(static_cast<const int32_t*>(values)[index]);
  case CARQUET_PHYSICAL_INT64:
    return Value::INT64(static_cast<const int64_t*>(values)[index]);
  case CARQUET_PHYSICAL_FLOAT:
    return Value::FLOAT(static_cast<const float*>(values)[index]);
  case CARQUET_PHYSICAL_DOUBLE:
    return Value::DOUBLE(static_cast<const double*>(values)[index]);
  case CARQUET_PHYSICAL_BOOLEAN:
    return Value::BOOLEAN(
        static_cast<const uint8_t*>(values)[index] != 0);
  case CARQUET_PHYSICAL_BYTE_ARRAY: {
    const auto& bytes =
        static_cast<const carquet_byte_array_t*>(values)[index];
    return Value::STRING(std::string(reinterpret_cast<const char*>(bytes.data),
                                     static_cast<size_t>(bytes.length)));
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported map entry physical type: " +
        std::string(carquet_physical_type_name(physical)));
  }
}

void appendMapRowsFromKeyValueBatch(
    execution::ListColumnBuilder& map_builder, const DataType& struct_type,
    carquet_physical_type_t key_physical, const carquet_logical_type_t* key_logical,
    carquet_physical_type_t val_physical, const carquet_logical_type_t* val_logical,
    const void* key_values, const void* val_values, const int16_t* key_def,
    const int16_t* val_def, const int16_t* key_rep, const int16_t* val_rep,
    int64_t count, int16_t key_max_def, int16_t val_max_def) {
  int64_t i = 0;
  int64_t dense_key = 0;
  int64_t dense_val = 0;
  while (i < count) {
    if (key_def[i] == 0) {
      map_builder.push_back_elem(Value::LIST(struct_type, {}));
      ++i;
      continue;
    }

    std::vector<Value> entries;
    while (i < count) {
      if (key_rep[i] == 0 && !entries.empty()) {
        break;
      }
      if (key_def[i] == key_max_def && val_def[i] == val_max_def) {
        entries.push_back(Value::STRUCT({
            extractFlatParquetValue(key_physical, key_logical, key_values,
                                    dense_key),
            extractFlatParquetValue(val_physical, val_logical, val_values,
                                    dense_val),
        }));
      }
      if (key_def[i] == key_max_def) {
        ++dense_key;
      }
      if (val_def[i] == val_max_def) {
        ++dense_val;
      }
      ++i;
    }
    map_builder.push_back_elem(Value::LIST(struct_type, std::move(entries)));
  }
}

std::shared_ptr<execution::IContextColumn> readCarquetMapColumn(
    carquet_reader_t* reader, const CarquetProjectedColumn& column) {
  if (column.map_key_leaf < 0 || column.map_value_leaf < 0) {
    THROW_NOT_SUPPORTED_EXCEPTION("Incomplete Carquet MAP column projection");
  }

  const carquet_schema_t* schema = carquet_reader_schema(reader);
  const carquet_physical_type_t key_physical =
      carquet_schema_column_type(schema, column.map_key_leaf);
  const carquet_physical_type_t val_physical =
      carquet_schema_column_type(schema, column.map_value_leaf);
  const carquet_logical_type_t* key_logical =
      carquetLeafLogicalType(schema, column.map_key_leaf);
  const carquet_logical_type_t* val_logical =
      carquetLeafLogicalType(schema, column.map_value_leaf);
  const int16_t key_max_def =
      carquet_schema_max_def_level(schema, column.map_key_leaf);
  const int16_t val_max_def =
      carquet_schema_max_def_level(schema, column.map_value_leaf);

  const DataType struct_type = DataType::Struct({
      listElementDataType(key_physical, key_logical),
      listElementDataType(val_physical, val_logical),
  });
  execution::ListColumnBuilder map_builder(struct_type);
  carquet_error_t err = CARQUET_ERROR_INIT;
  const int32_t num_row_groups = carquet_reader_num_row_groups(reader);

  const auto type_size = [](carquet_physical_type_t physical) -> size_t {
    switch (physical) {
    case CARQUET_PHYSICAL_BOOLEAN:
      return 1;
    case CARQUET_PHYSICAL_INT32:
    case CARQUET_PHYSICAL_FLOAT:
      return 4;
    case CARQUET_PHYSICAL_INT64:
    case CARQUET_PHYSICAL_DOUBLE:
      return 8;
    case CARQUET_PHYSICAL_BYTE_ARRAY:
      return sizeof(carquet_byte_array_t);
    default:
      return 0;
    }
  };

  const size_t key_size = type_size(key_physical);
  const size_t val_size = type_size(val_physical);
  if (key_size == 0 || val_size == 0) {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported Carquet MAP entry types");
  }

  std::vector<uint8_t> key_buffer(4096 * key_size);
  std::vector<uint8_t> val_buffer(4096 * val_size);
  std::vector<int16_t> key_def(4096);
  std::vector<int16_t> val_def(4096);
  std::vector<int16_t> key_rep(4096);
  std::vector<int16_t> val_rep(4096);

  for (int32_t rg = 0; rg < num_row_groups; ++rg) {
    carquet_column_reader_t* key_col =
        carquet_reader_get_column(reader, rg, column.map_key_leaf, &err);
    if (key_col == nullptr) {
      throwCarquetError(err, "Failed to open Carquet map key reader");
    }
    carquet_column_reader_t* val_col =
        carquet_reader_get_column(reader, rg, column.map_value_leaf, &err);
    if (val_col == nullptr) {
      carquet_column_reader_free(key_col);
      throwCarquetError(err, "Failed to open Carquet map value reader");
    }

    while (true) {
      const int64_t count = carquet_column_read_batch(
          key_col, key_buffer.data(), static_cast<int64_t>(key_def.size()),
          key_def.data(), key_rep.data());
      if (count <= 0) {
        break;
      }
      const int64_t val_count = carquet_column_read_batch(
          val_col, val_buffer.data(), static_cast<int64_t>(val_def.size()),
          val_def.data(), val_rep.data());
      if (val_count != count) {
        carquet_column_reader_free(key_col);
        carquet_column_reader_free(val_col);
        THROW_IO_EXCEPTION("Carquet MAP key/value batch size mismatch");
      }
      appendMapRowsFromKeyValueBatch(
          map_builder, struct_type, key_physical, key_logical, val_physical,
          val_logical, key_buffer.data(), val_buffer.data(), key_def.data(),
          val_def.data(), key_rep.data(), val_rep.data(), count, key_max_def,
          val_max_def);
    }

    carquet_column_reader_free(key_col);
    carquet_column_reader_free(val_col);
  }

  return map_builder.finish();
}

std::shared_ptr<execution::IContextColumn> readCarquetStructColumn(
    carquet_reader_t* reader, const CarquetProjectedColumn& column) {
  if (column.struct_leaf_indices.empty()) {
    THROW_NOT_SUPPORTED_EXCEPTION("Incomplete Carquet STRUCT column projection");
  }

  const carquet_schema_t* schema = carquet_reader_schema(reader);
  std::vector<std::shared_ptr<execution::IContextColumn>> children;
  std::vector<DataType> child_types;
  children.reserve(column.struct_leaf_indices.size());
  child_types.reserve(column.struct_leaf_indices.size());

  for (int32_t leaf : column.struct_leaf_indices) {
    const auto physical = carquet_schema_column_type(schema, leaf);
    const auto* logical = carquetLeafLogicalType(schema, leaf);
    auto child = readCarquetFlatColumn(reader, leaf, physical, logical);
    child_types.push_back(child->elem_type());
    children.push_back(std::move(child));
  }

  const DataType struct_type = DataType::Struct(child_types);
  execution::StructColumnBuilder builder(struct_type);
  const size_t num_rows = children.empty() ? 0 : children[0]->size();
  for (size_t row = 0; row < num_rows; ++row) {
    std::vector<execution::Value> values;
    values.reserve(children.size());
    for (const auto& child : children) {
      values.push_back(child->get_elem(row));
    }
    builder.push_back_elem(
        execution::Value::STRUCT(struct_type, std::move(values)));
  }
  return builder.finish();
}

execution::DataChunk readCarquetProjectedColumns(
    carquet_reader_t* reader,
    const std::vector<CarquetProjectedColumn>& projected) {
  execution::DataChunk chunk;
  for (size_t i = 0; i < projected.size(); ++i) {
    const auto& column = projected[i];
    const auto physical = carquet_schema_column_type(
        carquet_reader_schema(reader), column.leaf_index);
    const auto* logical =
        carquetLeafLogicalType(carquet_reader_schema(reader), column.leaf_index);
    switch (column.layout) {
    case CarquetColumnLayout::kFlat:
      chunk.set(static_cast<int>(i),
                readCarquetFlatColumn(reader, column.leaf_index, physical,
                                      logical));
      break;
    case CarquetColumnLayout::kList:
      chunk.set(static_cast<int>(i),
                readCarquetListColumn(reader, column.leaf_index, physical,
                                      logical, column.max_rep_level,
                                      column.max_def_level));
      break;
    case CarquetColumnLayout::kMap:
      chunk.set(static_cast<int>(i), readCarquetMapColumn(reader, column));
      break;
    case CarquetColumnLayout::kStruct:
      chunk.set(static_cast<int>(i), readCarquetStructColumn(reader, column));
      break;
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Carquet nested column layout is not supported yet for read");
    }
  }
  return chunk;
}

}  // namespace reader
}  // namespace neug
