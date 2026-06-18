/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "neug/utils/io/read/common/options.h"

#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
#include <parquet/properties.h>
#endif

namespace neug {
namespace reader {

struct ParquetParseOptions {
  Option<bool> buffered_stream =
      Option<bool>::BoolOption("BUFFERED_STREAM", true);
  Option<bool> pre_buffer =
      Option<bool>::BoolOption("PRE_BUFFER", false);
  Option<bool> enable_io_coalescing =
      Option<bool>::BoolOption("ENABLE_IO_COALESCING", true);
  Option<int64_t> row_batch_size =
      Option<int64_t>::Int64Option("PARQUET_BATCH_ROWS", 65536);
};

struct ParquetExportOptions {
  Option<std::string> compression =
      Option<std::string>::StringOption("COMPRESSION", "snappy");
  Option<int64_t> row_group_size =
      Option<int64_t>::Int64Option("ROW_GROUP_SIZE", 1048576);
  Option<bool> dictionary_encoding =
      Option<bool>::BoolOption("DICTIONARY_ENCODING", true);
};

/// Parquet reader configuration shared by Arrow and Carquet backends.
struct ParquetReadOptions {
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  std::shared_ptr<::parquet::ReaderProperties> reader_properties;
  std::shared_ptr<::parquet::ArrowReaderProperties> arrow_reader_properties;
#endif
  int64_t batch_size = 65536;
  int64_t io_buffer_size = 8192;
  bool use_threads = true;
  bool use_mmap = true;
  bool pre_buffer = false;
  bool enable_io_coalescing = true;
};

class ParquetOptionsBuilder {
 public:
  explicit ParquetOptionsBuilder(std::shared_ptr<ReadSharedState> state)
      : state_(std::move(state)) {}

  ParquetReadOptions build() const;

 private:
  std::shared_ptr<ReadSharedState> state_;
};

/// Parquet writer configuration shared by Arrow and Carquet backends.
struct ParquetWriteOptions {
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  std::shared_ptr<::parquet::WriterProperties> writer_properties;
#endif
  std::string compression = "snappy";
  int64_t row_group_size = 1048576;
  bool dictionary_encoding = true;
};

class ParquetExportOptionsBuilder {
 public:
  explicit ParquetExportOptionsBuilder(const options_t& options)
      : options_(options) {}

  ParquetWriteOptions build() const;

 private:
  options_t options_;
};

}  // namespace reader
}  // namespace neug
