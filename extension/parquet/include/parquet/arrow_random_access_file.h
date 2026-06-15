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

#include <arrow/io/interfaces.h>

#include "neug/utils/io/stream/input_stream.h"

namespace neug {
namespace parquet_adapt {

/// Adapts core io::RandomAccessFile to arrow::io::RandomAccessFile for libparquet.
std::shared_ptr<arrow::io::RandomAccessFile> wrapRandomAccessFile(
    std::unique_ptr<io::RandomAccessFile> file);

}  // namespace parquet_adapt
}  // namespace neug
