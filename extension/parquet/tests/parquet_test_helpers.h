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

}  // namespace test
}  // namespace neug
