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

#include <cstdint>
#include <string>
#include <vector>

#include "s3_client_config.h"

namespace neug {
namespace extension {
namespace s3 {

/// curl + AWS SigV4 S3 client for ListObjectsV2 and ranged GET.
class S3Client {
 public:
  explicit S3Client(S3ClientConfig config);

  std::vector<std::string> listObjectKeys(const std::string& bucket,
                                          const std::string& prefix) const;

  int64_t headObjectSize(const std::string& bucket,
                         const std::string& key) const;

  int64_t readObjectRange(const std::string& bucket, const std::string& key,
                          int64_t offset, int64_t length, void* out) const;

 private:
  S3ClientConfig config_;
};

}  // namespace s3
}  // namespace extension
}  // namespace neug
