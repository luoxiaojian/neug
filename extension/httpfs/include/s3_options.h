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

// This header is only used when Arrow is enabled.
#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <string>
#include "neug/utils/exception/exception.h"
#include "s3_config_keys.h"

namespace neug {
namespace extension {
namespace s3 {

/**
 * @brief Resolve and apply a TLS CA bundle path to Arrow's global FileSystem
 *        options so that Arrow's S3FileSystem propagates it into the AWS SDK
 *        ClientConfiguration::caFile (and caPath).
 *
 * To make the extension portable across distros,
 * we explicitly resolve a CA bundle path at runtime and feed it into AWS SDK's
 * client config via Arrow's FileSystemGlobalOptions.
 *
 * Resolution priority (first existing readable file wins):
 *   1. Env var SSL_CERT_FILE
 *   2. Env var CURL_CA_BUNDLE
 *   3. Env var AWS_CA_BUNDLE
 *   4. Common distro paths:
 *      - /etc/ssl/certs/ca-certificates.crt   (Debian/Ubuntu)
 *      - /etc/pki/tls/certs/ca-bundle.crt     (CentOS/RHEL/Fedora)
 *      - /etc/ssl/cert.pem                    (Alpine/macOS)
 *      - /etc/ssl/ca-bundle.pem               (OpenSUSE)
 *
 * Idempotent and safe to call multiple times.
 */
void InitializeArrowTlsOptions();

/**
 * @brief S3 Options Builder - Thin wrapper around Arrow's S3Options
 * 
 * This builder follows Arrow/AWS SDK's native design with minimal additions:
 * 
 * Configuration Names (OSS-style canonical + AWS-compatible aliases):
 * - OSS_ENDPOINT or AWS_ENDPOINT_URL or ENDPOINT_OVERRIDE: Custom S3-compatible endpoint (OSS, MinIO)
 * - OSS_REGION or AWS_DEFAULT_REGION: AWS/OSS region (e.g., "us-east-1", "oss-cn-beijing")
 * - OSS_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID: Access key (for Explicit credentials mode)
 * - OSS_ACCESS_KEY_SECRET or AWS_SECRET_ACCESS_KEY: Secret key (for Explicit credentials mode)
 * 
 * NeuG-specific options:
 * - CREDENTIALS_KIND: Which Arrow S3CredentialsKind to use (Explicit/Anonymous/Default)
 * 
 * Credential Resolution (follows Arrow's design):
 * 
 * When CREDENTIALS_KIND=Explicit:
 *   - Reads OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET (or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) from schema.options ONLY
 * 
 * When CREDENTIALS_KIND=Default:
 *   - Arrow SDK's default credential provider chain takes over:
 *     1. AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY environment variables
 *     2. ~/.aws/credentials and ~/.aws/config files  
 *     3. EC2 instance metadata (IAM role)
 *     4. ECS task role
 *   - NeuG does NOT interfere with this chain
 * 
 * When CREDENTIALS_KIND=Anonymous:
 *   - No credentials used (for public buckets only)
 * 
 * OSS-Specific Handling (automatic when OSS endpoint detected):
 *   - force_virtual_addressing = true (OSS requires virtual hosted-style)
 *   - Auto-detect region from endpoint (e.g., oss-cn-hangzhou.aliyuncs.com -> oss-cn-hangzhou)
 *
 */
class S3OptionsBuilder {
 public:
  /**
   * @brief Constructs an S3OptionsBuilder with the given file schema
   * @param schema The file schema containing S3 paths and configuration
   */
  explicit S3OptionsBuilder(const reader::FileSchema& schema)
      : schema_(schema) {}
  
  /**
   * @brief Build Arrow S3Options from schema configuration
   * 
   * This method:
   * 1. Starts from arrow::fs::S3Options::Defaults()
   * 2. Applies endpoint override (AWS_ENDPOINT_URL or ENDPOINT_OVERRIDE)
   * 3. Applies region (AWS_DEFAULT_REGION, or auto-detect from OSS endpoint)
   * 4. Configures credentials based on CREDENTIALS_KIND
   * 5. Applies OSS-specific settings if OSS endpoint detected
   * 
   * @return Configured Arrow S3Options instance
   */
  arrow::fs::S3Options build() const;

 private:
  const reader::FileSchema& schema_;
  S3ParseOptions parse_options_{};
  
  // Helper methods for configuration resolution
  // Note: Endpoint/Region resolution uses getOptionWithEnv() helper (defined in .cc)
  // to support environment variable fallback for deployment flexibility
  
  /**
   * @brief Resolve endpoint override from options or environment
   * Checks: AWS_ENDPOINT_URL > ENDPOINT_OVERRIDE > ENDPOINT (legacy)
   * @return Endpoint URL (empty if using default AWS S3)
   */
  std::string resolveEndpoint() const;
  
  /**
   * @brief Resolve AWS region from options, environment, or endpoint
   * Checks: AWS_DEFAULT_REGION > AWS_REGION > REGION (legacy) > auto-detect from endpoint
   * @param endpoint The resolved endpoint URL
   * @return AWS region string
   */
  std::string resolveRegion(const std::string& endpoint) const;
  
  /**
   * @brief Resolve credentials kind from options
   * 
   * Resolution logic (follows Arrow's design):
   * 1. If CREDENTIALS_KIND explicitly specified in schema.options -> use that value
   * 2. Otherwise -> Default (Arrow's default credential mode)
   * 
   * Note: CREDENTIALS_KIND is NOT read from environment variables.
   * Use LOAD FROM query inline options to specify credentials kind:
   *   LOAD FROM "oss://bucket/file" (CREDENTIALS_KIND='Anonymous')
   * 
   * @return Arrow S3CredentialsKind enum value
   */
  arrow::fs::S3CredentialsKind resolveCredentialsKind() const;
  
  /**
   * @brief Configure S3Options credentials using Arrow's official API
   * @param s3_options The S3Options to configure
   * @param is_oss Whether the endpoint is OSS (for Default mode OSS env var support)
   */
  void configureCredentials(arrow::fs::S3Options& s3_options, bool is_oss) const;
  
  /**
   * @brief Apply OSS-specific settings if OSS endpoint detected
   * - Sets force_virtual_addressing = true
   * @param s3_options The S3Options to configure
   * @param endpoint The endpoint URL
   */
  void applyOSSSettings(arrow::fs::S3Options& s3_options, const std::string& endpoint) const;
  
  /**
   * @brief Detect if endpoint is Alibaba Cloud OSS
   * @param endpoint The endpoint URL
   * @return true if endpoint contains "aliyuncs.com"
   */
  static bool isOSSEndpoint(const std::string& endpoint);
  
  /**
   * @brief Extract region from OSS endpoint pattern
   * @param endpoint OSS endpoint (e.g., "oss-cn-beijing.aliyuncs.com")
   * @return Region string (e.g., "oss-cn-beijing")
   */
  static std::string extractOSSRegion(const std::string& endpoint);
};

}  // namespace s3
}  // namespace extension
}  // namespace neug

#endif  // NEUG_USE_ARROW
