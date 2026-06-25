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

#include "parquet/parquet_decoder.h"

namespace neug {
namespace reader {

/// libparquet + minimal libarrow decode path (current production backend).
class ArrowParquetDecoder : public IParquetDecoder {
 public:
  result<std::shared_ptr<EntrySchema>> inferSchema(
      fsys::FileSystem& fs, const std::vector<std::string>& paths,
      const ParquetReadOptions& options) override;

  std::vector<std::shared_ptr<IDataChunkSupplier>> openSuppliers(
      fsys::FileSystem& fs, const ReadSharedState& state,
      const ParquetReadOptions& options, bool batch_read) override;
};

}  // namespace reader
}  // namespace neug
