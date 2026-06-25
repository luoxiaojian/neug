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

#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {

enum class CarquetColumnLayout { kFlat, kList, kMap, kStruct };

struct CarquetProjectedColumn {
  int32_t leaf_index = -1;
  CarquetColumnLayout layout = CarquetColumnLayout::kFlat;
  int16_t max_def_level = 0;
  int16_t max_rep_level = 0;
  int32_t map_key_leaf = -1;
  int32_t map_value_leaf = -1;
  std::vector<int32_t> struct_leaf_indices;
};

const carquet_logical_type_t* carquetLeafLogicalType(
    const carquet_schema_t* schema, int32_t leaf_index);

/// Maps Carquet physical/logical types to NeuG common::DataType.
std::shared_ptr<::common::DataType> carquetColumnToCommonType(
    carquet_physical_type_t physical,
    const carquet_logical_type_t* logical);

result<std::shared_ptr<EntrySchema>> carquetSchemaToEntrySchema(
    const carquet_schema_t* schema);

/// Resolves a top-level Parquet column name to its primary leaf index.
int32_t carquetSchemaFindTopLevelColumn(const carquet_schema_t* schema,
                                        const char* name);

CarquetColumnLayout carquetClassifyLeafColumn(const carquet_schema_t* schema,
                                              int32_t leaf_index);

std::vector<CarquetProjectedColumn> resolveCarquetProjectedColumns(
    const carquet_schema_t* schema, const std::vector<std::string>& names);

std::vector<int32_t> resolveCarquetColumnIndices(
    const carquet_schema_t* schema, const std::vector<std::string>& names);

}  // namespace reader
}  // namespace neug
