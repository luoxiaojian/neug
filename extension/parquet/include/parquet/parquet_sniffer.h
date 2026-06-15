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

#include "neug/utils/io/read/common/sniffer.h"
#include "parquet/parquet_reader.h"

namespace neug {
namespace reader {

class ParquetSniffer : public Sniffer {
 public:
  explicit ParquetSniffer(std::shared_ptr<ParquetReader> reader)
      : reader_(std::move(reader)) {}

  result<std::shared_ptr<EntrySchema>> sniff() override;

 private:
  std::shared_ptr<ParquetReader> reader_;
};

using ArrowSniffer = ParquetSniffer;

}  // namespace reader
}  // namespace neug
