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

#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/vfs/file_system.h"
#include "parquet/parquet_options.h"

namespace neug {
namespace reader {

struct CarquetExportColumn {
  int32_t leaf_index = -1;
  bool is_list = false;
  bool is_struct = false;
  bool is_map = false;
  int32_t map_key_leaf = -1;
  int32_t map_value_leaf = -1;
  std::vector<int32_t> struct_leaf_indices;
};

/// Builds a Carquet schema from a QueryResponse table layout.
carquet_schema_t* buildCarquetSchemaFromQueryResponse(
    const QueryResponse* table,
    const std::shared_ptr<EntrySchema>& entry_schema,
    std::vector<CarquetExportColumn>& columns, carquet_error_t* error);

carquet_writer_options_t buildCarquetWriterOptions(
    const ParquetWriteOptions& options);

std::string resolveCarquetExportPath(fsys::FileSystem& fs,
                                     const std::string& path);

}  // namespace reader
}  // namespace neug
