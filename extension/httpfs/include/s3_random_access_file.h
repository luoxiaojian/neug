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

#include <memory>
#include <string>

#include "neug/utils/io/stream/input_stream.h"
#include "s3_client.h"

namespace neug {
namespace extension {
namespace s3 {

/// Random-access reader for a single S3 object (core IO, no Arrow).
class S3RandomAccessFile : public io::RandomAccessFile {
 public:
  S3RandomAccessFile(std::shared_ptr<S3Client> client, std::string bucket,
                     std::string key);

  int64_t Tell() const override;
  int64_t Size() override;
  neug::Status Seek(int64_t position) override;
  neug::Status Read(int64_t nbytes, void* out, int64_t* bytes_read) override;
  neug::Status Close() override;
  bool Closed() const override;

 private:
  std::shared_ptr<S3Client> client_;
  std::string bucket_;
  std::string key_;
  int64_t size_ = -1;
  int64_t position_ = 0;
  bool closed_ = false;
};

}  // namespace s3
}  // namespace extension
}  // namespace neug
