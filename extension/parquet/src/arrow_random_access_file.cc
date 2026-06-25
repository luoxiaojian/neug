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

#include "parquet/arrow_random_access_file.h"

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace neug {
namespace parquet_adapt {
namespace {

class NeugRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
  explicit NeugRandomAccessFile(std::unique_ptr<io::RandomAccessFile> file)
      : file_(std::move(file)) {}

  arrow::Result<int64_t> GetSize() override {
    if (file_ == nullptr || file_->Closed()) {
      return arrow::Status::IOError("File is closed");
    }
    return file_->Size();
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    if (file_ == nullptr || file_->Closed()) {
      return arrow::Status::IOError("File is closed");
    }
    int64_t bytes_read = 0;
    auto status = file_->Read(nbytes, out, &bytes_read);
    if (!status.ok()) {
      return arrow::Status::IOError(status.ToString());
    }
    return bytes_read;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
    if (bytes_read < nbytes) {
      RETURN_NOT_OK(buffer->Resize(bytes_read));
      buffer->ZeroPadding();
    }
    return buffer;
  }

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes,
                                void* out) override {
    ARROW_RETURN_NOT_OK(Seek(position));
    return Read(nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                       int64_t nbytes) override {
    ARROW_RETURN_NOT_OK(Seek(position));
    return Read(nbytes);
  }

  arrow::Status Seek(int64_t position) override {
    if (file_ == nullptr || file_->Closed()) {
      return arrow::Status::IOError("File is closed");
    }
    auto status = file_->Seek(position);
    if (!status.ok()) {
      return arrow::Status::IOError(status.ToString());
    }
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    if (file_ == nullptr || file_->Closed()) {
      return arrow::Status::IOError("File is closed");
    }
    return file_->Tell();
  }

  arrow::Status Close() override {
    if (file_ != nullptr) {
      auto status = file_->Close();
      file_.reset();
      if (!status.ok()) {
        return arrow::Status::IOError(status.ToString());
      }
    }
    return arrow::Status::OK();
  }

  bool closed() const override {
    return file_ == nullptr || file_->Closed();
  }

 private:
  std::unique_ptr<io::RandomAccessFile> file_;
};

}  // namespace

std::shared_ptr<arrow::io::RandomAccessFile> wrapRandomAccessFile(
    std::unique_ptr<io::RandomAccessFile> file) {
  return std::make_shared<NeugRandomAccessFile>(std::move(file));
}

}  // namespace parquet_adapt
}  // namespace neug
