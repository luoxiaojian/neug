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

#include "s3_client.h"

#include <curl/curl.h>
#include <glog/logging.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <vector>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace extension {
namespace s3 {
namespace {

std::once_flag curl_init_flag;

void ensureCurlGlobalInit() {
  std::call_once(curl_init_flag, []() {
    CURLcode res = curl_global_init(CURL_GLOBAL_ALL);
    if (res != CURLE_OK) {
      THROW_IO_EXCEPTION("Failed to initialize CURL globally: " +
                         std::string(curl_easy_strerror(res)));
    }
  });
}

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

std::string toHex(const unsigned char* data, size_t len) {
  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  for (size_t i = 0; i < len; ++i) {
    oss << std::setw(2) << static_cast<int>(data[i]);
  }
  return oss.str();
}

std::string sha256Hex(const std::string& data) {
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash);
  return toHex(hash, SHA256_DIGEST_LENGTH);
}

std::string hmacSha256Raw(const std::string& key, const std::string& data) {
  unsigned char out[EVP_MAX_MD_SIZE];
  unsigned int len = 0;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const unsigned char*>(data.data()), data.size(), out,
       &len);
  return std::string(reinterpret_cast<char*>(out), len);
}

std::string getSignatureKey(const std::string& secret, const std::string& date,
                            const std::string& region,
                            const std::string& service) {
  auto k_date = hmacSha256Raw("AWS4" + secret, date);
  auto k_region = hmacSha256Raw(k_date, region);
  auto k_service = hmacSha256Raw(k_region, service);
  return hmacSha256Raw(k_service, "aws4_request");
}

bool isUnreserved(char ch) {
  return std::isalnum(static_cast<unsigned char>(ch)) || ch == '-' || ch == '_' ||
         ch == '.' || ch == '~';
}

std::string uriEncode(const std::string& value, bool encode_slash) {
  std::ostringstream encoded;
  encoded << std::hex << std::uppercase << std::setfill('0');
  for (unsigned char ch : value) {
    if (isUnreserved(static_cast<char>(ch)) || (ch == '/' && !encode_slash)) {
      encoded << static_cast<char>(ch);
    } else {
      encoded << '%' << std::setw(2) << static_cast<int>(ch);
    }
  }
  return encoded.str();
}

std::string canonicalQueryString(
    const std::map<std::string, std::string>& query) {
  std::ostringstream oss;
  bool first = true;
  for (const auto& [key, value] : query) {
    if (!first) {
      oss << '&';
    }
    first = false;
    oss << uriEncode(key, true) << '=' << uriEncode(value, true);
  }
  return oss.str();
}

bool useVirtualAddressing(const S3ClientConfig& config) {
  if (config.force_virtual_addressing) {
    return true;
  }
  return config.endpoint_override.empty();
}

std::string defaultAwsHost(const S3ClientConfig& config,
                           const std::string& bucket) {
  if (config.region == "us-east-1") {
    return bucket + ".s3.amazonaws.com";
  }
  return bucket + ".s3." + config.region + ".amazonaws.com";
}

struct RequestParts {
  std::string url;
  std::string host;
  std::string canonical_uri;
  std::map<std::string, std::string> query;
  std::map<std::string, std::string> signed_headers;
};

RequestParts buildObjectRequest(const S3ClientConfig& config,
                                const std::string& bucket,
                                const std::string& key,
                                const std::string& range_header) {
  RequestParts parts;
  const std::string encoded_key = uriEncode(key, false);
  if (useVirtualAddressing(config)) {
    parts.host = config.endpoint_override.empty()
                     ? defaultAwsHost(config, bucket)
                     : bucket + "." + config.endpoint_override;
    parts.canonical_uri = "/" + encoded_key;
    parts.url = config.scheme + "://" + parts.host + parts.canonical_uri;
  } else {
    parts.host = config.endpoint_override;
    parts.canonical_uri = "/" + bucket + "/" + encoded_key;
    parts.url = config.scheme + "://" + parts.host + parts.canonical_uri;
  }
  parts.signed_headers["host"] = parts.host;
  if (!range_header.empty()) {
    parts.signed_headers["range"] = range_header;
  }
  return parts;
}

RequestParts buildListRequest(const S3ClientConfig& config,
                              const std::string& bucket,
                              const std::string& prefix,
                              const std::string& continuation_token) {
  RequestParts parts;
  parts.query["list-type"] = "2";
  if (!prefix.empty()) {
    parts.query["prefix"] = prefix;
  }
  if (!continuation_token.empty()) {
    parts.query["continuation-token"] = continuation_token;
  }

  if (useVirtualAddressing(config)) {
    parts.host = config.endpoint_override.empty()
                     ? defaultAwsHost(config, bucket)
                     : bucket + "." + config.endpoint_override;
    parts.canonical_uri = "/";
  } else {
    parts.host = config.endpoint_override;
    parts.canonical_uri = "/" + bucket;
  }

  const std::string query = canonicalQueryString(parts.query);
  parts.url = config.scheme + "://" + parts.host + parts.canonical_uri;
  if (!query.empty()) {
    parts.url += "?" + query;
  }
  parts.signed_headers["host"] = parts.host;
  return parts;
}

std::string amzDateNow(std::string* out_date_stamp) {
  time_t now = time(nullptr);
  struct tm tm_buf;
  gmtime_r(&now, &tm_buf);
  char amz_date[17];
  strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &tm_buf);
  char date_stamp[9];
  strftime(date_stamp, sizeof(date_stamp), "%Y%m%d", &tm_buf);
  *out_date_stamp = date_stamp;
  return amz_date;
}

std::string buildAuthorizationHeader(
    const S3ClientConfig& config, const std::string& method,
    const RequestParts& parts, const std::string& payload_hash,
    const std::string& amz_date, const std::string& date_stamp) {
  std::map<std::string, std::string> headers = parts.signed_headers;
  headers["x-amz-content-sha256"] = payload_hash;
  headers["x-amz-date"] = amz_date;
  if (!config.session_token.empty()) {
    headers["x-amz-security-token"] = config.session_token;
  }

  std::ostringstream canonical_headers;
  std::ostringstream signed_header_names;
  bool first = true;
  for (const auto& [name, value] : headers) {
    canonical_headers << name << ':' << value << '\n';
    if (!first) {
      signed_header_names << ';';
    }
    first = false;
    signed_header_names << name;
  }

  const std::string canonical_request =
      method + '\n' + parts.canonical_uri + '\n' +
      canonicalQueryString(parts.query) + '\n' + canonical_headers.str() + '\n' +
      signed_header_names.str() + '\n' + payload_hash;

  const std::string credential_scope =
      date_stamp + '/' + config.region + "/s3/aws4_request";
  const std::string string_to_sign =
      "AWS4-HMAC-SHA256\n" + amz_date + '\n' + credential_scope + '\n' +
      sha256Hex(canonical_request);

  const std::string signing_key =
      getSignatureKey(config.secret_key, date_stamp, config.region, "s3");
  const std::string signature = toHex(
      reinterpret_cast<const unsigned char*>(hmacSha256Raw(signing_key, string_to_sign).data()),
      32);

  std::ostringstream auth;
  auth << "AWS4-HMAC-SHA256 Credential=" << config.access_key << '/'
       << credential_scope << ", SignedHeaders=" << signed_header_names.str()
       << ", Signature=" << signature;
  return auth.str();
}

/// RAII guard for curl_slist to prevent leaks on exceptions.
struct CurlSlistGuard {
  struct curl_slist* list = nullptr;
  CurlSlistGuard() = default;
  ~CurlSlistGuard() {
    if (list) {
      curl_slist_free_all(list);
    }
  }
  CurlSlistGuard(CurlSlistGuard&& o) noexcept : list(o.list) { o.list = nullptr; }
  CurlSlistGuard& operator=(CurlSlistGuard&&) = delete;
  CurlSlistGuard(const CurlSlistGuard&) = delete;
  CurlSlistGuard& operator=(const CurlSlistGuard&) = delete;
};

/// Streaming write context — directs response body to a caller-provided
/// buffer instead of accumulating in HttpResponse::body.
struct StreamWriteContext {
  void* out = nullptr;          // Destination buffer
  int64_t capacity = 0;         // Max bytes that can be written
  int64_t written = 0;          // Bytes written so far
  bool active = false;          // Only stream when status is 200/206
};

struct HttpResponse {
  long status_code = 0;
  std::string body;
  int64_t content_length = -1;  // Populated when header callback is active.
  // When non-null and active, body data is streamed directly to this buffer
  // instead of accumulating in `body`.
  StreamWriteContext* stream_ctx = nullptr;
};

size_t writeCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  size_t real_size = size * nmemb;
  auto* response = static_cast<HttpResponse*>(userp);
  if (response->stream_ctx && response->stream_ctx->active) {
    auto* ctx = response->stream_ctx;
    int64_t remaining = ctx->capacity - ctx->written;
    size_t to_write =
        std::min(static_cast<size_t>(remaining), real_size);
    if (to_write > 0) {
      std::memcpy(static_cast<char*>(ctx->out) + ctx->written, contents,
                  to_write);
      ctx->written += static_cast<int64_t>(to_write);
    }
  } else {
    response->body.append(static_cast<char*>(contents), real_size);
  }
  return real_size;
}

size_t headerCallback(char* buffer, size_t size, size_t nitems, void* userdata) {
  size_t total = size * nitems;
  auto* response = static_cast<HttpResponse*>(userdata);
  const std::string header(buffer, total);
  constexpr const char* kPrefix = "Content-Length:";
  if (header.rfind(kPrefix, 0) == 0) {
    try {
      response->content_length = std::stoll(header.substr(strlen(kPrefix)));
    } catch (...) {
    }
  }
  // Parse the HTTP status line (e.g. "HTTP/1.1 206 Partial Content") to
  // control whether the write callback streams or buffers.
  if (header.rfind("HTTP/", 0) == 0 && response->stream_ctx) {
    size_t sp = header.find(' ');
    if (sp != std::string::npos) {
      try {
        long code = std::stol(header.substr(sp + 1));
        response->stream_ctx->active = (code == 200 || code == 206);
      } catch (...) {
      }
    }
  }
  return total;
}

HttpResponse performRequest(const S3ClientConfig& config,
                            const std::string& method, RequestParts parts,
                            const std::string& range_header = "",
                            StreamWriteContext* stream_ctx = nullptr) {
  if (config.credentials_kind != NativeS3CredentialsKind::Anonymous &&
      (config.access_key.empty() || config.secret_key.empty())) {
    THROW_IO_EXCEPTION(
        "S3 credentials are missing. Set CREDENTIALS_KIND='Explicit' with "
        "access keys, or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.");
  }

  ensureCurlGlobalInit();

  std::string date_stamp;
  const std::string amz_date = amzDateNow(&date_stamp);
  const std::string payload_hash =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  HttpResponse response;
  response.stream_ctx = stream_ctx;
  CURL* curl = curl_easy_init();
  if (!curl) {
    THROW_IO_EXCEPTION("Failed to initialize CURL handle for S3 request");
  }

  struct curl_slist* header_list = nullptr;
  header_list = curl_slist_append(header_list, ("host: " + parts.host).c_str());
  header_list =
      curl_slist_append(header_list, ("x-amz-date: " + amz_date).c_str());
  header_list = curl_slist_append(
      header_list, ("x-amz-content-sha256: " + payload_hash).c_str());
  if (!config.session_token.empty()) {
    header_list = curl_slist_append(
        header_list,
        ("x-amz-security-token: " + config.session_token).c_str());
  }
  if (!range_header.empty()) {
    header_list =
        curl_slist_append(header_list, ("Range: " + range_header).c_str());
  }
  if (config.credentials_kind != NativeS3CredentialsKind::Anonymous) {
    const std::string auth = buildAuthorizationHeader(
        config, method, parts, payload_hash, amz_date, date_stamp);
    header_list =
        curl_slist_append(header_list, ("Authorization: " + auth).c_str());
  }

  curl_easy_setopt(curl, CURLOPT_URL, parts.url.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
  if (stream_ctx) {
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response);
  }
  if (method == "HEAD") {
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  }
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                   static_cast<long>(config.connect_timeout));
  curl_easy_setopt(curl, CURLOPT_TIMEOUT,
                   static_cast<long>(config.request_timeout));
  if (!config.tls_ca_file.empty()) {
    curl_easy_setopt(curl, CURLOPT_CAINFO, config.tls_ca_file.c_str());
  }

  CURLcode res = curl_easy_perform(curl);
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.status_code);
  curl_slist_free_all(header_list);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    THROW_IO_EXCEPTION("S3 request failed: " +
                       std::string(curl_easy_strerror(res)));
  }
  return response;
}

std::vector<std::string> parseListObjectKeys(const std::string& xml) {
  std::vector<std::string> keys;
  const std::string open_tag = "<Key>";
  const std::string close_tag = "</Key>";
  size_t pos = 0;
  while ((pos = xml.find(open_tag, pos)) != std::string::npos) {
    pos += open_tag.size();
    size_t end = xml.find(close_tag, pos);
    if (end == std::string::npos) {
      break;
    }
    keys.push_back(xml.substr(pos, end - pos));
    pos = end + close_tag.size();
  }
  return keys;
}

bool parseIsTruncated(const std::string& xml) {
  const std::string tag = "<IsTruncated>true</IsTruncated>";
  return xml.find(tag) != std::string::npos;
}

std::string parseNextContinuationToken(const std::string& xml) {
  const std::string open_tag = "<NextContinuationToken>";
  const std::string close_tag = "</NextContinuationToken>";
  size_t pos = xml.find(open_tag);
  if (pos == std::string::npos) {
    return "";
  }
  pos += open_tag.size();
  size_t end = xml.find(close_tag, pos);
  if (end == std::string::npos) {
    return "";
  }
  return xml.substr(pos, end - pos);
}

}  // namespace

S3Client::S3Client(S3ClientConfig config) : config_(std::move(config)) {
  ensureCurlGlobalInit();
}

std::vector<std::string> S3Client::listObjectKeys(const std::string& bucket,
                                                  const std::string& prefix) const {
  std::vector<std::string> all_keys;
  std::string continuation_token;
  do {
    auto parts = buildListRequest(config_, bucket, prefix, continuation_token);
    auto response = performRequest(config_, "GET", std::move(parts));
    if (response.status_code < 200 || response.status_code >= 300) {
      THROW_IO_EXCEPTION("S3 ListObjectsV2 failed with HTTP " +
                         std::to_string(response.status_code) + ": " +
                         response.body.substr(0, 512));
    }
    auto keys = parseListObjectKeys(response.body);
    all_keys.insert(all_keys.end(), keys.begin(), keys.end());
    if (!parseIsTruncated(response.body)) {
      break;
    }
    continuation_token = parseNextContinuationToken(response.body);
  } while (!continuation_token.empty());

  return all_keys;
}

int64_t S3Client::headObjectSize(const std::string& bucket,
                                 const std::string& key) const {
  auto parts = buildObjectRequest(config_, bucket, key, "");
  HttpResponse head_response;

  ensureCurlGlobalInit();
  std::string date_stamp;
  const std::string amz_date = amzDateNow(&date_stamp);
  const std::string payload_hash =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  CURL* curl = curl_easy_init();
  if (!curl) {
    THROW_IO_EXCEPTION("Failed to initialize CURL handle for S3 HEAD");
  }

  struct curl_slist* header_list = nullptr;
  header_list = curl_slist_append(header_list, ("host: " + parts.host).c_str());
  header_list =
      curl_slist_append(header_list, ("x-amz-date: " + amz_date).c_str());
  header_list = curl_slist_append(
      header_list, ("x-amz-content-sha256: " + payload_hash).c_str());
  if (!config_.session_token.empty()) {
    header_list = curl_slist_append(
        header_list,
        ("x-amz-security-token: " + config_.session_token).c_str());
  }
  if (config_.credentials_kind != NativeS3CredentialsKind::Anonymous) {
    const std::string auth = buildAuthorizationHeader(
        config_, "HEAD", parts, payload_hash, amz_date, date_stamp);
    header_list =
        curl_slist_append(header_list, ("Authorization: " + auth).c_str());
  }

  curl_easy_setopt(curl, CURLOPT_URL, parts.url.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, &head_response);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                   static_cast<long>(config_.connect_timeout));
  curl_easy_setopt(curl, CURLOPT_TIMEOUT,
                   static_cast<long>(config_.request_timeout));
  if (!config_.tls_ca_file.empty()) {
    curl_easy_setopt(curl, CURLOPT_CAINFO, config_.tls_ca_file.c_str());
  }

  CURLcode res = curl_easy_perform(curl);
  long status_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);
  curl_slist_free_all(header_list);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    THROW_IO_EXCEPTION("S3 HEAD request failed: " +
                       std::string(curl_easy_strerror(res)));
  }
  if (status_code < 200 || status_code >= 300) {
    THROW_IO_EXCEPTION("S3 HEAD failed with HTTP " +
                     std::to_string(status_code));
  }
  return head_response.content_length;
}

int64_t S3Client::readObjectRange(const std::string& bucket,
                                  const std::string& key, int64_t offset,
                                  int64_t length, void* out) const {
  if (length <= 0) {
    return 0;
  }
  const std::string range_header = "bytes=" + std::to_string(offset) + '-' +
                                   std::to_string(offset + length - 1);
  auto parts = buildObjectRequest(config_, bucket, key, range_header);
  // Use streaming to write directly to the caller's buffer, avoiding
  // double-buffering through HttpResponse::body.
  StreamWriteContext stream_ctx;
  stream_ctx.out = out;
  stream_ctx.capacity = length;
  auto response = performRequest(config_, "GET", std::move(parts),
                                 range_header, &stream_ctx);
  if (response.status_code == 416) {
    return 0;
  }
  if (response.status_code != 200 && response.status_code != 206) {
    THROW_IO_EXCEPTION("S3 GET failed with HTTP " +
                       std::to_string(response.status_code) + ": " +
                       response.body.substr(0, 512));
  }
  return stream_ctx.written;
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
