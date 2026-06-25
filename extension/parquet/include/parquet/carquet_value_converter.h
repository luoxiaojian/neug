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
#pragma once

#include <carquet/carquet.h>

#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/data_chunk.h"
#include "neug/utils/io/vfs/file_system.h"
#include "parquet/carquet_type_converter.h"
#include "parquet/parquet_options.h"

namespace neug {
namespace reader {

/// RAII handle for a Carquet reader opened from VFS (local mmap or in-memory).
class CarquetReaderHandle {
 public:
  CarquetReaderHandle() = default;
  ~CarquetReaderHandle() { close(); }

  CarquetReaderHandle(CarquetReaderHandle&& other) noexcept;
  CarquetReaderHandle& operator=(CarquetReaderHandle&& other) noexcept;
  CarquetReaderHandle(const CarquetReaderHandle&) = delete;
  CarquetReaderHandle& operator=(const CarquetReaderHandle&) = delete;

  carquet_reader_t* get() const { return reader; }
  explicit operator bool() const { return reader != nullptr; }
  void close();

  /// Read-only accessors for testing.
  bool buffer_empty() const { return owned_buffer.empty(); }
  size_t buffer_size() const { return owned_buffer.size(); }
  /// For testing only: sets a fake reader pointer without opening a file.
  void set_reader_for_test(carquet_reader_t* r) { reader = r; }

 private:
  friend CarquetReaderHandle openCarquetReader(fsys::FileSystem&,
                                               const std::string&,
                                               const ParquetReadOptions&);

  carquet_reader_t* reader = nullptr;
  std::vector<uint8_t> owned_buffer;
};

CarquetReaderHandle openCarquetReader(fsys::FileSystem& fs,
                                      const std::string& path,
                                      const ParquetReadOptions& options);

/// Converts an INT96 Julian-day-based timestamp to epoch milliseconds.
/// Clamps to INT64_MIN/INT64_MAX on overflow; validates nanos_since_midnight.
int64_t int96ToMillis(const carquet_int96_t& value);

std::shared_ptr<execution::IContextColumn> carquetBatchColumnToValueColumn(
    const carquet_row_batch_t* batch, int32_t batch_column_index,
    carquet_physical_type_t physical,
    const carquet_logical_type_t* logical);

std::shared_ptr<execution::IContextColumn> readCarquetFlatColumn(
    carquet_reader_t* reader, int32_t leaf_index,
    carquet_physical_type_t physical, const carquet_logical_type_t* logical);

std::shared_ptr<execution::IContextColumn> readCarquetListColumn(
    carquet_reader_t* reader, int32_t leaf_index,
    carquet_physical_type_t physical, const carquet_logical_type_t* logical,
    int16_t max_rep_level, int16_t max_def_level);

execution::DataChunk readCarquetProjectedColumns(
    carquet_reader_t* reader,
    const std::vector<CarquetProjectedColumn>& projected);

execution::DataChunk carquetBatchToDataChunk(
    const carquet_row_batch_t* batch,
    const std::vector<carquet_physical_type_t>& physical_types,
    const std::vector<const carquet_logical_type_t*>& logical_types);

}  // namespace reader
}  // namespace neug
