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

#include <curl/curl.h>
#include <memory>
#include <string>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/utils/io/stream/input_stream.h"

namespace neug {
namespace extension {
namespace http {

void ensureHttpCurlGlobalInit();

/// libcurl-backed random access file for HTTP/HTTPS reads (core IO, no Arrow).
class HttpRandomAccessFile : public io::RandomAccessFile {
 public:
  HttpRandomAccessFile(const std::string& url,
                       const common::case_insensitive_map_t<std::string>& options);
  ~HttpRandomAccessFile() override;

  int64_t Tell() const override;
  int64_t Size() override;
  neug::Status Seek(int64_t position) override;
  neug::Status Read(int64_t nbytes, void* out, int64_t* bytes_read) override;
  neug::Status Close() override;
  bool Closed() const override;

  neug::Status readAt(int64_t position, int64_t nbytes, void* out,
                      int64_t* bytes_read);

 private:
  neug::Status initializeFileSize();
  neug::Status readRange(int64_t offset, int64_t length, void* buffer,
                         int64_t* bytes_read);
  void setupCURLHandle(CURL* curl);

  std::string url_;
  common::case_insensitive_map_t<std::string> options_;
  CURL* curl_handle_;
  int64_t file_size_;
  int64_t position_;
  bool closed_;
  std::string bearer_token_;
  std::vector<std::string> custom_headers_;
  struct curl_slist* header_list_;
};

}  // namespace http
}  // namespace extension
}  // namespace neug
