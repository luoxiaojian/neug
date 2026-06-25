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
 * See the License for the License governing permissions and limitations.
 */

#include "s3_random_access_file.h"

#include "neug/utils/exception/exception.h"

namespace neug {
namespace extension {
namespace s3 {

S3RandomAccessFile::S3RandomAccessFile(std::shared_ptr<S3Client> client,
                                       std::string bucket, std::string key)
    : client_(std::move(client)),
      bucket_(std::move(bucket)),
      key_(std::move(key)) {
  if (!client_) {
    THROW_IO_EXCEPTION("S3 client is null");
  }
  size_ = client_->headObjectSize(bucket_, key_);
}

int64_t S3RandomAccessFile::Tell() const { return position_; }

int64_t S3RandomAccessFile::Size() { return size_; }

neug::Status S3RandomAccessFile::Seek(int64_t position) {
  if (closed_) {
    return neug::Status(StatusCode::ERR_IO_ERROR, "File is closed");
  }
  if (position < 0) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Negative seek position");
  }
  position_ = position;
  return neug::Status::OK();
}

neug::Status S3RandomAccessFile::Read(int64_t nbytes, void* out,
                                      int64_t* bytes_read) {
  if (bytes_read == nullptr) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "bytes_read output is null");
  }
  *bytes_read = 0;
  if (closed_) {
    return neug::Status(StatusCode::ERR_IO_ERROR, "File is closed");
  }
  if (nbytes <= 0) {
    return neug::Status::OK();
  }
  try {
    *bytes_read =
        client_->readObjectRange(bucket_, key_, position_, nbytes, out);
    position_ += *bytes_read;
    return neug::Status::OK();
  } catch (const exception::Exception& error) {
    return neug::Status(StatusCode::ERR_IO_ERROR, error.what());
  }
}

neug::Status S3RandomAccessFile::Close() {
  closed_ = true;
  return neug::Status::OK();
}

bool S3RandomAccessFile::Closed() const { return closed_; }

}  // namespace s3
}  // namespace extension
}  // namespace neug
