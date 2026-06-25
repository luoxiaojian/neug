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

#include <string>

#include "neug/utils/io/read/common/schema.h"

namespace neug {
namespace extension {
namespace s3 {

enum class NativeS3CredentialsKind { Explicit, Anonymous, Default };

/// Native S3 client configuration (no Arrow types).
struct S3ClientConfig {
  std::string region = "us-east-1";
  std::string endpoint_override;
  std::string scheme = "https";
  bool force_virtual_addressing = false;
  NativeS3CredentialsKind credentials_kind = NativeS3CredentialsKind::Default;
  std::string access_key;
  std::string secret_key;
  std::string session_token;
  double connect_timeout = 5.0;
  double request_timeout = 30.0;
  std::string tls_ca_file;
};

S3ClientConfig buildS3ClientConfig(const reader::FileSchema& schema);

std::string resolveTlsCaFilePath();

}  // namespace s3
}  // namespace extension
}  // namespace neug
