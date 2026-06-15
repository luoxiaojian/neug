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

#include "http_random_access_file.h"

#include "http_options.h"

#include <glog/logging.h>
#include <algorithm>
#include <cstring>
#include <mutex>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace extension {
namespace http {
namespace {

std::once_flag curl_init_flag;

void ensureCurlGlobalInit() {
  std::call_once(curl_init_flag, []() {
    CURLcode res = curl_global_init(CURL_GLOBAL_ALL);
    if (res != CURLE_OK) {
      THROW_IO_EXCEPTION("Failed to initialize CURL globally: " +
                         std::string(curl_easy_strerror(res)));
    }
    LOG(INFO) << "CURL global initialization completed";
  });
}

size_t WriteCallbackDirect(void* contents, size_t size, size_t nmemb,
                           void* userp) {
  size_t real_size = size * nmemb;
  auto* info = static_cast<std::pair<void*, size_t>*>(userp);

  size_t to_copy = std::min(real_size, info->second);
  if (to_copy > 0) {
    std::memcpy(info->first, contents, to_copy);
    info->first = static_cast<uint8_t*>(info->first) + to_copy;
    info->second -= to_copy;
  }

  if (real_size > to_copy) {
    LOG(WARNING) << "WriteCallbackDirect: buffer full, aborting transfer. "
                 << "real_size=" << real_size << ", copied=" << to_copy
                 << ", discarding " << (real_size - to_copy) << " bytes";
    return 0;
  }

  return real_size;
}

size_t HeaderCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  return size * nmemb;
}

}  // namespace

void ensureHttpCurlGlobalInit() { ensureCurlGlobalInit(); }

HttpRandomAccessFile::HttpRandomAccessFile(
    const std::string& url,
    const common::case_insensitive_map_t<std::string>& options)
    : url_(url),
      options_(options),
      curl_handle_(nullptr),
      file_size_(-1),
      position_(0),
      closed_(false),
      header_list_(nullptr) {
  ensureCurlGlobalInit();

  curl_handle_ = curl_easy_init();
  if (!curl_handle_) {
    THROW_IO_EXCEPTION("Failed to initialize CURL handle");
  }

  auto bearer_it = options_.find(HTTPConfigOptionKeys::kBearerToken);
  if (bearer_it != options_.end()) {
    bearer_token_ = bearer_it->second;
  }

  auto auth_header_it = options_.find(HTTPConfigOptionKeys::kAuthorizationHeader);
  if (auth_header_it != options_.end() && bearer_token_.empty()) {
    custom_headers_.push_back("Authorization: " + auth_header_it->second);
  } else if (!bearer_token_.empty()) {
    custom_headers_.push_back("Authorization: Bearer " + bearer_token_);
  }

  auto headers_it = options_.find(HTTPConfigOptionKeys::kCustomHeaders);
  if (headers_it != options_.end()) {
    std::string headers_str = headers_it->second;
    size_t pos = 0;
    while (pos < headers_str.size()) {
      size_t sep = headers_str.find(';', pos);
      std::string header = (sep == std::string::npos)
                               ? headers_str.substr(pos)
                               : headers_str.substr(pos, sep - pos);

      if (!header.empty()) {
        custom_headers_.push_back(header);
      }

      pos = (sep == std::string::npos) ? headers_str.size() : sep + 1;
    }
  }

  auto status = initializeFileSize();
  if (!status.ok()) {
    curl_easy_cleanup(curl_handle_);
    THROW_IO_EXCEPTION("Failed to initialize HTTP file: " + status.ToString());
  }
}

HttpRandomAccessFile::~HttpRandomAccessFile() {
  if (header_list_) {
    curl_slist_free_all(header_list_);
  }
  if (curl_handle_) {
    curl_easy_cleanup(curl_handle_);
  }
}

neug::Status HttpRandomAccessFile::initializeFileSize() {
  CURL* curl = curl_easy_init();
  if (!curl) {
    return neug::Status(StatusCode::ERR_IO_ERROR,
                        "Failed to initialize CURL for HEAD request");
  }

  setupCURLHandle(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HeaderCallback);

  CURLcode res = curl_easy_perform(curl);

  if (res != CURLE_OK) {
    curl_easy_cleanup(curl);
    return neug::Status(StatusCode::ERR_IO_ERROR,
                          "HEAD request failed: " +
                              std::string(curl_easy_strerror(res)));
  }

  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  if (http_code < 200 || http_code >= 300) {
    curl_easy_cleanup(curl);
    return neug::Status(StatusCode::ERR_IO_ERROR,
                        "HEAD request returned HTTP " +
                            std::to_string(http_code) + " for " + url_);
  }

  double content_length = -1;
  res = curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD,
                          &content_length);

  if (res == CURLE_OK && content_length >= 0) {
    file_size_ = static_cast<int64_t>(content_length);
    curl_easy_cleanup(curl);
    return neug::Status::OK();
  }

  curl_easy_cleanup(curl);

  curl = curl_easy_init();
  if (!curl) {
    return neug::Status(StatusCode::ERR_IO_ERROR,
                        "Failed to initialize CURL for RANGE request");
  }
  setupCURLHandle(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
  curl_easy_setopt(curl, CURLOPT_RANGE, "0-0");
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HeaderCallback);

  res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    curl_easy_cleanup(curl);
    return neug::Status(StatusCode::ERR_IO_ERROR,
                        "Failed to determine file size: " +
                            std::string(curl_easy_strerror(res)));
  }

  http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  curl_easy_cleanup(curl);
  if (http_code < 200 || http_code >= 300) {
    return neug::Status(StatusCode::ERR_IO_ERROR,
                        "RANGE request returned HTTP " +
                            std::to_string(http_code) + " for " + url_);
  }

  file_size_ = -1;
  LOG(WARNING) << "Could not determine file size for " << url_;
  return neug::Status::OK();
}

void HttpRandomAccessFile::setupCURLHandle(CURL* curl) {
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);
  curl_easy_setopt(curl, CURLOPT_HEADER, 0L);

  auto verify_it = options_.find(HTTPConfigOptionKeys::kVerifySSL);
  bool verify_ssl = HTTPConfigDefaults::kVerifySSLDefault;
  if (verify_it != options_.end()) {
    std::string v = verify_it->second;
    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
    if (v == "true" || v == "1" || v == "yes" || v == "on") {
      verify_ssl = true;
    } else if (v == "false" || v == "0" || v == "no" || v == "off") {
      verify_ssl = false;
    } else {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Invalid VERIFY_SSL value '" + verify_it->second +
          "'. Expected 'true'/'false', '1'/'0', 'yes'/'no', or 'on'/'off'.");
    }
  }
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, verify_ssl ? 1L : 0L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, verify_ssl ? 2L : 0L);

  auto ca_cert_it = options_.find(HTTPConfigOptionKeys::kCACertFile);
  if (ca_cert_it != options_.end()) {
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_it->second.c_str());
  }

  auto connect_timeout_it = options_.find(HTTPConfigOptionKeys::kConnectTimeout);
  int connect_timeout = HTTPConfigDefaults::kConnectTimeoutDefault;
  if (connect_timeout_it != options_.end()) {
    try {
      connect_timeout = std::stoi(connect_timeout_it->second);
    } catch (const std::exception& e) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Invalid CONNECT_TIMEOUT value: '" + connect_timeout_it->second +
          "'. Must be an integer. Error: " + e.what());
    }
  }
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connect_timeout);

  auto request_timeout_it = options_.find(HTTPConfigOptionKeys::kRequestTimeout);
  int request_timeout = HTTPConfigDefaults::kRequestTimeoutDefault;
  if (request_timeout_it != options_.end()) {
    try {
      request_timeout = std::stoi(request_timeout_it->second);
    } catch (const std::exception& e) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Invalid REQUEST_TIMEOUT value: '" + request_timeout_it->second +
          "'. Must be an integer. Error: " + e.what());
    }
  }
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, request_timeout);

  auto proxy_it = options_.find(HTTPConfigOptionKeys::kHTTPProxy);
  if (proxy_it != options_.end()) {
    curl_easy_setopt(curl, CURLOPT_PROXY, proxy_it->second.c_str());

    auto proxy_user_it = options_.find(HTTPConfigOptionKeys::kHTTPProxyUsername);
    auto proxy_pass_it = options_.find(HTTPConfigOptionKeys::kHTTPProxyPassword);
    if (proxy_user_it != options_.end() && proxy_pass_it != options_.end()) {
      std::string userpass =
          proxy_user_it->second + ":" + proxy_pass_it->second;
      curl_easy_setopt(curl, CURLOPT_PROXYUSERPWD, userpass.c_str());
    }
  }

  if (!custom_headers_.empty()) {
    if (header_list_) {
      curl_slist_free_all(header_list_);
      header_list_ = nullptr;
    }
    for (const auto& header : custom_headers_) {
      header_list_ = curl_slist_append(header_list_, header.c_str());
    }
  }
  if (header_list_) {
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list_);
  }
}

neug::Status HttpRandomAccessFile::readRange(int64_t offset, int64_t length,
                                             void* buffer, int64_t* bytes_read) {
  if (bytes_read == nullptr) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "bytes_read output is null");
  }
  *bytes_read = 0;
  if (closed_) {
    return neug::Status(StatusCode::ERR_IO_ERROR, "File is closed");
  }
  if (length == 0) {
    return neug::Status::OK();
  }

  curl_easy_reset(curl_handle_);
  setupCURLHandle(curl_handle_);
  curl_easy_setopt(curl_handle_, CURLOPT_URL, url_.c_str());

  std::string range_value = std::to_string(offset) + "-" +
                            std::to_string(offset + length - 1);
  curl_easy_setopt(curl_handle_, CURLOPT_RANGE, range_value.c_str());

  std::pair<void*, size_t> write_info{buffer, static_cast<size_t>(length)};
  curl_easy_setopt(curl_handle_, CURLOPT_WRITEFUNCTION, WriteCallbackDirect);
  curl_easy_setopt(curl_handle_, CURLOPT_WRITEDATA, &write_info);

  CURLcode res = curl_easy_perform(curl_handle_);

  if (res != CURLE_OK) {
    return neug::Status(StatusCode::ERR_IO_ERROR,
                        "HTTP Range request failed: " +
                            std::string(curl_easy_strerror(res)));
  }

  long response_code = 0;
  curl_easy_getinfo(curl_handle_, CURLINFO_RESPONSE_CODE, &response_code);

  int64_t bytes_received = length - static_cast<int64_t>(write_info.second);

  if (response_code == 416) {
    return neug::Status::OK();
  }
  if (response_code == 206) {
    if (bytes_received < length) {
      LOG(INFO) << "HTTP Range request returned partial data. "
                << "Requested: " << length << " bytes, "
                << "Received: " << bytes_received << " bytes";
    }
    *bytes_read = bytes_received;
    return neug::Status::OK();
  }
  if (response_code == 200) {
    LOG(WARNING) << "Server returned 200 instead of 206 — Range requests "
                 << "may not be supported. Received " << bytes_received
                 << " of " << length << " requested bytes.";
    *bytes_read = bytes_received;
    return neug::Status::OK();
  }
  return neug::Status(StatusCode::ERR_IO_ERROR,
                      "HTTP Range request failed with status " +
                          std::to_string(response_code));
}

int64_t HttpRandomAccessFile::Tell() const { return position_; }

int64_t HttpRandomAccessFile::Size() { return file_size_; }

neug::Status HttpRandomAccessFile::Seek(int64_t position) {
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

neug::Status HttpRandomAccessFile::Read(int64_t nbytes, void* out,
                                        int64_t* bytes_read) {
  return readAt(position_, nbytes, out, bytes_read);
}

neug::Status HttpRandomAccessFile::readAt(int64_t position, int64_t nbytes,
                                          void* out, int64_t* bytes_read) {
  int64_t read_count = 0;
  auto status = readRange(position, nbytes, out, &read_count);
  if (!status.ok()) {
    return status;
  }
  if (position == position_) {
    position_ += read_count;
  }
  if (bytes_read != nullptr) {
    *bytes_read = read_count;
  }
  return neug::Status::OK();
}

neug::Status HttpRandomAccessFile::Close() {
  closed_ = true;
  return neug::Status::OK();
}

bool HttpRandomAccessFile::Closed() const { return closed_; }

}  // namespace http
}  // namespace extension
}  // namespace neug
