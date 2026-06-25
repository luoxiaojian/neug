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

#include "parquet/parquet_reader.h"
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

#include <memory>
#include <string>
#include <vector>

#include <arrow/type.h>

#include "neug/utils/io/read/common/file_reader.h"
#include "neug/utils/io/vfs/file_system.h"
#include "parquet_options.h"

namespace neug {

class IDataChunkSupplier;

namespace reader {

/// Reads Parquet via libparquet + VFS (no Arrow Dataset/Acero).
class ArrowReader : public FileReader {
 public:
  ArrowReader(std::shared_ptr<ReadSharedState> sharedState,
              std::unique_ptr<ParquetOptionsBuilder> optionsBuilder,
              std::unique_ptr<fsys::FileSystem> fileSystem)
      : sharedState_(std::move(sharedState)),
        fileSystem_(std::move(fileSystem)),
        optionsBuilder_(std::move(optionsBuilder)) {}
  ~ArrowReader() override = default;

  std::shared_ptr<IDataChunkSupplier> read() override;

  result<std::shared_ptr<EntrySchema>> inferSchema() override;

 protected:
  result<std::shared_ptr<EntrySchema>> convertArrowSchemaToEntrySchema(
      const std::shared_ptr<arrow::Schema>& arrowSchema);

  std::shared_ptr<IDataChunkSupplier> full_read(
      const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers,
      const std::vector<std::string>& file_column_names);

  std::shared_ptr<IDataChunkSupplier> batch_read(
      const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers);

  std::shared_ptr<ReadSharedState> sharedState_;
  std::unique_ptr<fsys::FileSystem> fileSystem_;
  std::unique_ptr<ParquetOptionsBuilder> optionsBuilder_;
};

}  // namespace reader
}  // namespace neug
