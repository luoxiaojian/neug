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

#include <gtest/gtest.h>

#include <string>

namespace neug {
namespace test {

void writeSimpleParquetFile(const std::string& directory,
                            const std::string& filename);

void writeNumericTypesParquetFile(const std::string& directory,
                                  const std::string& filename);

void writePruningParquetFile(const std::string& directory,
                             const std::string& filename);

void writeFilterParquetFile(const std::string& directory,
                            const std::string& filename);

void writeBatchModeParquetFile(const std::string& directory,
                               const std::string& filename);

void writeCombinedParquetFile(const std::string& directory,
                              const std::string& filename);

void writeMultiFileParquet(const std::string& directory,
                           const std::string& filename, int start_id,
                           int count);

void writeListParquetFile(const std::string& directory,
                          const std::string& filename);

void writeMapParquetFile(const std::string& directory,
                         const std::string& filename);

void writeStructParquetFile(const std::string& directory,
                            const std::string& filename);

void writeInt96TimestampParquetFile(const std::string& directory,
                                    const std::string& filename);

// Creates a Parquet file with a list column where lists have varying lengths
// and include null elements. Used to test value_idx accumulation across lists.
// Row 1: ["A", "B", "C"]  (3 non-null elements)
// Row 2: ["D"]              (1 non-null element)
// Row 3: ["E", "F"]         (2 non-null elements)
void writeMultiListParquetFile(const std::string& directory,
                               const std::string& filename);

// Creates a Parquet file with mixed column types in a specific order:
// tags (list<string>), id (int64), name (string)
// Used to verify schema column ordering is preserved.
void writeMixedColumnOrderParquetFile(const std::string& directory,
                                      const std::string& filename);

// Creates a Parquet file with a map column where one entry has a null value.
// Row 1: {"a": "abc", "b": null}  — only the non-null entry should survive.
void writeMapWithNullValueParquetFile(const std::string& directory,
                                      const std::string& filename);

}  // namespace test
}  // namespace neug
