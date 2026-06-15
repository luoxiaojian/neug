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
#include "parquet/parquet_options.h"

namespace neug {
namespace reader {

/// Opens a Carquet reader from VFS (local mmap path or in-memory buffer).
struct CarquetReaderHandle {
  carquet_reader_t* reader = nullptr;
  std::vector<uint8_t> owned_buffer;
};

CarquetReaderHandle openCarquetReader(fsys::FileSystem& fs,
                                      const std::string& path,
                                      const ParquetReadOptions& options);

void closeCarquetReader(CarquetReaderHandle& handle);

std::shared_ptr<execution::IContextColumn> carquetBatchColumnToValueColumn(
    const carquet_row_batch_t* batch, int32_t batch_column_index,
    carquet_physical_type_t physical,
    const carquet_logical_type_t* logical);

execution::DataChunk carquetBatchToDataChunk(
    const carquet_row_batch_t* batch,
    const std::vector<carquet_physical_type_t>& physical_types,
    const std::vector<const carquet_logical_type_t*>& logical_types);

}  // namespace reader
}  // namespace neug
