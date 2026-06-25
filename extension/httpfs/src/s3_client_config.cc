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

#include "s3_client_config.h"

#include "s3_config_keys.h"

#include <glog/logging.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"

namespace neug {
namespace extension {
namespace s3 {
namespace {

bool fileExistsAndReadable(const std::string& path) {
  if (path.empty()) {
    return false;
  }
  struct stat st;
  if (::stat(path.c_str(), &st) != 0) {
    return false;
  }
  if (!S_ISREG(st.st_mode)) {
    return false;
  }
  return ::access(path.c_str(), R_OK) == 0;
}

template <typename T>
T getOptionWithEnv(const reader::options_t& options,
                   const reader::Option<T>& opt,
                   std::initializer_list<const char*> option_keys,
                   std::initializer_list<const char*> env_keys) {
  bool option_key_present = false;
  std::string option_value;

  for (const char* key : option_keys) {
    auto it = options.find(key);
    if (it != options.end()) {
      option_key_present = true;
      option_value = it->second;
      break;
    }
  }

  if (option_key_present) {
    reader::options_t temp;
    temp.emplace(opt.getKey(), option_value);
    return opt.get(temp);
  }

  for (const char* env_key : env_keys) {
    const char* env_val = std::getenv(env_key);
    if (env_val && std::strlen(env_val) > 0) {
      reader::options_t env_options;
      env_options.emplace(opt.getKey(), std::string(env_val));
      return opt.get(env_options);
    }
  }

  reader::options_t empty;
  return opt.get(empty);
}

bool isOSSEndpoint(const std::string& endpoint) {
  return endpoint.find("aliyuncs.com") != std::string::npos;
}

std::string extractOSSRegion(const std::string& endpoint) {
  std::string ep = endpoint;
  size_t protocol_pos = ep.find("://");
  if (protocol_pos != std::string::npos) {
    ep = ep.substr(protocol_pos + 3);
  }
  if (ep.find("oss-") == 0) {
    size_t dot_pos = ep.find('.');
    if (dot_pos != std::string::npos) {
      return ep.substr(0, dot_pos);
    }
  }
  return "oss-cn-hangzhou";
}

NativeS3CredentialsKind resolveNativeCredentialsKind(
    const reader::options_t& options) {
  S3ParseOptions parse_options;
  std::string kind_str = parse_options.credentials_kind.get(options);
  std::transform(kind_str.begin(), kind_str.end(), kind_str.begin(), ::tolower);

  if (kind_str == S3CredentialsKindValues::kExplicit) {
    return NativeS3CredentialsKind::Explicit;
  }
  if (kind_str == S3CredentialsKindValues::kAnonymous) {
    return NativeS3CredentialsKind::Anonymous;
  }
  if (kind_str == S3CredentialsKindValues::kDefault) {
    return NativeS3CredentialsKind::Default;
  }
  if (kind_str == S3CredentialsKindValues::kRole ||
      kind_str == S3CredentialsKindValues::kWebIdentity) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CREDENTIALS_KIND='" + kind_str + "' is not supported yet.");
  }
  THROW_INVALID_ARGUMENT_EXCEPTION("Invalid CREDENTIALS_KIND: " + kind_str);
}

void configureNativeCredentials(S3ClientConfig& config,
                                const reader::options_t& options,
                                bool is_oss) {
  config.credentials_kind = resolveNativeCredentialsKind(options);

  switch (config.credentials_kind) {
  case NativeS3CredentialsKind::Explicit: {
    for (const char* key :
         {S3ConfigOptionKeys::kAccessKeyCanonical,
          S3ConfigOptionKeys::kAccessKeyAws}) {
      auto it = options.find(key);
      if (it != options.end() && !it->second.empty()) {
        config.access_key = it->second;
        break;
      }
    }
    for (const char* key :
         {S3ConfigOptionKeys::kSecretAccessKeyCanonical,
          S3ConfigOptionKeys::kSecretAccessKeyAws}) {
      auto it = options.find(key);
      if (it != options.end() && !it->second.empty()) {
        config.secret_key = it->second;
        break;
      }
    }
    if (config.access_key.empty() || config.secret_key.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "CREDENTIALS_KIND=Explicit requires OSS_ACCESS_KEY_ID / "
          "OSS_ACCESS_KEY_SECRET (or AWS aliases) in options.");
    }
    break;
  }
  case NativeS3CredentialsKind::Anonymous:
    config.access_key.clear();
    config.secret_key.clear();
    break;
  case NativeS3CredentialsKind::Default:
    if (is_oss) {
      const char* oss_ak = std::getenv(S3ConfigOptionKeys::kAccessKeyCanonical);
      const char* oss_sk =
          std::getenv(S3ConfigOptionKeys::kSecretAccessKeyCanonical);
      if (oss_ak && oss_sk && std::strlen(oss_ak) > 0 &&
          std::strlen(oss_sk) > 0) {
        config.access_key = oss_ak;
        config.secret_key = oss_sk;
        break;
      }
    }
    if (const char* ak = std::getenv(S3ConfigOptionKeys::kAccessKeyAws)) {
      config.access_key = ak;
    }
    if (const char* sk = std::getenv(S3ConfigOptionKeys::kSecretAccessKeyAws)) {
      config.secret_key = sk;
    }
    if (const char* token = std::getenv("AWS_SESSION_TOKEN")) {
      config.session_token = token;
    }
    break;
  }
}

}  // namespace

std::string resolveTlsCaFilePath() {
  for (const char* env_key :
       {"SSL_CERT_FILE", "CURL_CA_BUNDLE", "AWS_CA_BUNDLE"}) {
    const char* v = std::getenv(env_key);
    if (v && std::strlen(v) > 0 && fileExistsAndReadable(v)) {
      return v;
    }
  }
  static const char* kCommonPaths[] = {
      "/etc/ssl/certs/ca-certificates.crt",
      "/etc/pki/tls/certs/ca-bundle.crt",
      "/etc/ssl/cert.pem",
      "/etc/ssl/ca-bundle.pem",
  };
  for (const char* path : kCommonPaths) {
    if (fileExistsAndReadable(path)) {
      return path;
    }
  }
  return "";
}

S3ClientConfig buildS3ClientConfig(const reader::FileSchema& schema) {
  const auto& options = schema.options;
  S3ParseOptions parse_options;
  S3ClientConfig config;

  std::string endpoint = getOptionWithEnv(
      options, parse_options.endpoint,
      {S3ConfigOptionKeys::kEndpointCanonical, S3ConfigOptionKeys::kEndpointAws,
       S3ConfigOptionKeys::kEndpointOverride},
      {S3ConfigOptionKeys::kEndpointCanonical, S3ConfigOptionKeys::kEndpointAws,
       S3ConfigOptionKeys::kEndpointOverride});

  config.region = getOptionWithEnv(
      options, parse_options.region,
      {S3ConfigOptionKeys::kRegionCanonical, S3ConfigOptionKeys::kRegionDefault},
      {S3ConfigOptionKeys::kRegionCanonical,
       S3ConfigOptionKeys::kRegionDefault});
  if (config.region.empty()) {
    if (!endpoint.empty() && isOSSEndpoint(endpoint)) {
      config.region = extractOSSRegion(endpoint);
    } else {
      config.region = "us-east-1";
    }
  }

  if (!endpoint.empty()) {
    if (endpoint.find("http://") == 0) {
      config.scheme = "http";
      config.endpoint_override = endpoint.substr(7);
    } else if (endpoint.find("https://") == 0) {
      config.scheme = "https";
      config.endpoint_override = endpoint.substr(8);
    } else {
      config.scheme = "https";
      config.endpoint_override = endpoint;
    }
    if (isOSSEndpoint(endpoint)) {
      config.force_virtual_addressing = true;
    }
  }

  configureNativeCredentials(config, options, isOSSEndpoint(endpoint));
  config.connect_timeout = parse_options.connect_timeout.get(options);
  config.request_timeout = parse_options.request_timeout.get(options);
  config.tls_ca_file = resolveTlsCaFilePath();
  return config;
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
