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

// Shared S3 configuration key/value constants used by both the native S3
// client (s3_client_config.cc) and the Arrow-based S3 options builder
// (s3_options.cc).  This header has NO Arrow dependency.

#include <string>
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/schema.h"

namespace neug {
namespace extension {
namespace s3 {

// Centralized definition of all S3-related configuration keys.
struct S3ConfigOptionKeys {
  // Endpoint: user may specify any of these via configs or env
  static constexpr const char* kEndpointCanonical = "OSS_ENDPOINT";           // NeuG canonical (OSS-style)
  static constexpr const char* kEndpointAws = "AWS_ENDPOINT_URL";         // AWS SDK standard
  static constexpr const char* kEndpointOverride = "ENDPOINT_OVERRIDE";   // Alternative endpoint name

  // Region: user may specify via configs or env
  static constexpr const char* kRegionCanonical = "OSS_REGION";              // NeuG canonical (OSS-style)
  static constexpr const char* kRegionDefault = "AWS_DEFAULT_REGION";     // AWS SDK standard

  // Credentials: access key ID and secret access key
  static constexpr const char* kCredentialsKind = "CREDENTIALS_KIND";  // controls S3CredentialsKind
  static constexpr const char* kAccessKeyCanonical = "OSS_ACCESS_KEY_ID";      // NeuG canonical (OSS-style)
  static constexpr const char* kAccessKeyAws = "AWS_ACCESS_KEY_ID";         // AWS-compatible alias
  static constexpr const char* kSecretAccessKeyCanonical = "OSS_ACCESS_KEY_SECRET"; // NeuG canonical (OSS-style)
  static constexpr const char* kSecretAccessKeyAws = "AWS_SECRET_ACCESS_KEY";   // AWS-compatible alias

  // Timeouts
  static constexpr const char* kConnectTimeout = "CONNECT_TIMEOUT";
  static constexpr const char* kRequestTimeout = "REQUEST_TIMEOUT";
};

// Valid values for CREDENTIALS_KIND option (case-insensitive)
struct S3CredentialsKindValues {
  static constexpr const char* kExplicit = "explicit";       // Use explicit AK/SK from options
  static constexpr const char* kAnonymous = "anonymous";     // No credentials (public buckets)
  static constexpr const char* kDefault = "default";         // Arrow's default chain (env/config/role)
  static constexpr const char* kRole = "role";               // Assume role (not yet supported)
  static constexpr const char* kWebIdentity = "webidentity"; // Web identity token (not yet supported)
};

// Logical S3 options schema (NeuG-level knobs)
struct S3ParseOptions {
  reader::Option<std::string> endpoint =
      reader::Option<std::string>::StringOption(S3ConfigOptionKeys::kEndpointCanonical, "");
  reader::Option<std::string> region =
      reader::Option<std::string>::StringOption(S3ConfigOptionKeys::kRegionCanonical, "");

  reader::Option<std::string> credentials_kind =
      reader::Option<std::string>::StringOption(S3ConfigOptionKeys::kCredentialsKind, "Default");

  reader::Option<double> connect_timeout =
      reader::Option<double>::DoubleOption(S3ConfigOptionKeys::kConnectTimeout, 5.0);
  reader::Option<double> request_timeout =
      reader::Option<double>::DoubleOption(S3ConfigOptionKeys::kRequestTimeout, 30.0);
};

}  // namespace s3
}  // namespace extension
}  // namespace neug
