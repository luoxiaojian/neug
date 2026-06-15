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

#include "parquet/parquet_options.h"

#include <glog/logging.h>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/read_state.h"

#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
#include <arrow/io/caching.h>
#include <parquet/arrow/reader.h>
#endif

namespace neug {
namespace reader {

ParquetReadOptions ParquetOptionsBuilder::build() const {
  if (!state_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }

  ParquetReadOptions options;
  const FileSchema& fileSchema = state_->schema.file;
  const auto& file_options = fileSchema.options;
  ParquetParseOptions parquet_opts;
  ReadOptions read_opts;

  options.batch_size = parquet_opts.row_batch_size.get(file_options);
  options.io_buffer_size = read_opts.batch_size.get(file_options);
  options.use_threads = read_opts.use_threads.get(file_options);
  options.pre_buffer = parquet_opts.pre_buffer.get(file_options);
  options.enable_io_coalescing =
      parquet_opts.enable_io_coalescing.get(file_options);
  options.use_mmap = !options.pre_buffer;

#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  options.reader_properties = std::make_shared<::parquet::ReaderProperties>();
  options.arrow_reader_properties =
      std::make_shared<::parquet::ArrowReaderProperties>();

  if (parquet_opts.buffered_stream.get(file_options)) {
    options.reader_properties->enable_buffered_stream();
  }
  options.reader_properties->set_buffer_size(options.io_buffer_size);
  options.arrow_reader_properties->set_batch_size(options.batch_size);
  options.arrow_reader_properties->set_use_threads(options.use_threads);
  options.arrow_reader_properties->set_pre_buffer(options.pre_buffer);

  if (options.enable_io_coalescing) {
    options.arrow_reader_properties->set_cache_options(
        arrow::io::CacheOptions::LazyDefaults());
  } else {
    options.arrow_reader_properties->set_cache_options(
        arrow::io::CacheOptions::Defaults());
  }
#endif

  return options;
}

}  // namespace reader
}  // namespace neug
