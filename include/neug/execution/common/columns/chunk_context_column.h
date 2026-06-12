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

#include <glog/logging.h>

#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace execution {

/// Streaming column backed by IDataChunkSupplier (ValueColumn chunks).
class ChunkStreamContextColumn : public IContextColumn {
 public:
  explicit ChunkStreamContextColumn(
      const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers)
      : suppliers_(suppliers) {}

  ~ChunkStreamContextColumn() override = default;

  std::string column_info() const override { return "ChunkStreamContextColumn"; }

  size_t size() const override { return suppliers_.size(); }

  const DataType& elem_type() const override { return type_; }

  ContextColumnType column_type() const override {
    return ContextColumnType::kChunkStream;
  }

  std::vector<std::shared_ptr<IDataChunkSupplier>> GetSuppliers() const {
    return suppliers_;
  }

  Value get_elem(size_t idx) const override {
    LOG(FATAL) << "get_elem not implemented for chunk stream column";
    return Value(DataType::SQLNULL);
  }

  bool is_optional() const override {
    LOG(FATAL) << "is_optional not implemented for chunk stream column";
    return false;
  }

 private:
  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers_;
  DataType type_;
};

class ChunkStreamContextColumnBuilder : public IContextColumnBuilder {
 public:
  explicit ChunkStreamContextColumnBuilder(
      const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers)
      : suppliers_(suppliers) {}
  ~ChunkStreamContextColumnBuilder() override = default;

  void reserve(size_t size) override {
    LOG(FATAL) << "not implemented for chunk stream column";
  }
  void push_back_elem(const Value& val) override {
    LOG(FATAL) << "not implemented for chunk stream column";
  }

  std::shared_ptr<IContextColumn> finish() override {
    return std::make_shared<ChunkStreamContextColumn>(suppliers_);
  }

 private:
  std::vector<std::shared_ptr<IDataChunkSupplier>> suppliers_;
};

}  // namespace execution
}  // namespace neug
