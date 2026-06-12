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

#include "neug/utils/io/read/common/reader_utils.h"

#include "neug/execution/common/columns/chunk_context_column.h"
#include "neug/utils/io/read/common/options.h"

namespace neug {
namespace reader {

execution::Context toContext(std::shared_ptr<IDataChunkSupplier> supplier,
                             const ReadSharedState& state,
                             size_t fallback_column_count) {
  execution::Context ctx;
  ReadOptions read_opts;
  const bool batch_read =
      read_opts.batch_read.get(state.schema.file.options);

  if (batch_read) {
    size_t column_count = state.columnNum();
    if (column_count == 0) {
      column_count = fallback_column_count;
    }
    execution::DataChunk chunk;
    for (size_t i = 0; i < column_count; ++i) {
      execution::ChunkStreamContextColumnBuilder builder({supplier});
      chunk.set(static_cast<int>(i), builder.finish());
    }
    ctx.append_chunk(std::move(chunk));
    return ctx;
  }

  while (supplier) {
    auto chunk = supplier->GetNextChunk();
    if (!chunk) {
      break;
    }
    ctx.append_chunk(std::move(*chunk));
  }
  return ctx;
}

}  // namespace reader
}  // namespace neug
