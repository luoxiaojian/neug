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

#include "http_filesystem.h"
#include "http_options.h"
#include "http_random_access_file.h"

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <curl/curl.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstring>
#include <sstream>
#include "neug/utils/io/vfs/file_system.h"

namespace neug {
namespace extension {
namespace http {

namespace {

arrow::Status toArrowStatus(const neug::Status& status) {
  if (status.ok()) {
    return arrow::Status::OK();
  }
  return arrow::Status::IOError(status.error_message());
}

}  // namespace

// ============================================================================
// HTTPURIComponents Implementation
// ============================================================================

HTTPURIComponents HTTPURIComponents::parse(const std::string& uri) {
  HTTPURIComponents components;
  
  // Find scheme
  size_t scheme_end = uri.find("://");
  if (scheme_end == std::string::npos) {
    THROW_IO_EXCEPTION("Invalid HTTP URI (missing scheme): " + uri);
  }
  
  components.scheme = uri.substr(0, scheme_end);
  if (components.scheme != "http" && components.scheme != "https") {
    THROW_IO_EXCEPTION("Invalid HTTP URI scheme (expected http or https): " + 
                       components.scheme);
  }
  
  // Parse authority and path
  size_t authority_start = scheme_end + 3;
  size_t path_start = uri.find('/', authority_start);
  
  std::string authority;
  if (path_start == std::string::npos) {
    authority = uri.substr(authority_start);
    components.path = "/";
  } else {
    authority = uri.substr(authority_start, path_start - authority_start);
    components.path = uri.substr(path_start);
  }
  
  // Parse host and port
  size_t port_sep = authority.find(':');
  if (port_sep == std::string::npos) {
    components.host = authority;
    // Default ports
    components.port = (components.scheme == "https") ? 443 : 80;
  } else {
    components.host = authority.substr(0, port_sep);
    std::string port_str = authority.substr(port_sep + 1);
    try {
      components.port = std::stoi(port_str);
    } catch (...) {
      THROW_IO_EXCEPTION("Invalid port number: " + port_str);
    }
  }
  
  return components;
}

std::string HTTPURIComponents::toURL() const {
  std::ostringstream oss;
  oss << scheme << "://" << host;
  
  // Only include port if non-default
  if ((scheme == "http" && port != 80) || 
      (scheme == "https" && port != 443)) {
    oss << ":" << port;
  }
  
  oss << path;
  return oss.str();
}

// ============================================================================
// HTTPRandomAccessFile (Arrow adapter)
// ============================================================================

HTTPRandomAccessFile::HTTPRandomAccessFile(
    const std::string& url,
    const common::case_insensitive_map_t<std::string>& options)
    : impl_(std::make_shared<HttpRandomAccessFile>(url, options)) {}

HTTPRandomAccessFile::HTTPRandomAccessFile(
    std::shared_ptr<HttpRandomAccessFile> impl)
    : impl_(std::move(impl)) {}

HTTPRandomAccessFile::~HTTPRandomAccessFile() = default;

arrow::Result<int64_t> HTTPRandomAccessFile::Tell() const {
  if (impl_->Closed()) {
    return arrow::Status::Invalid("File is closed");
  }
  return impl_->Tell();
}

arrow::Result<int64_t> HTTPRandomAccessFile::GetSize() {
  if (impl_->Closed()) {
    return arrow::Status::Invalid("File is closed");
  }
  return impl_->Size();
}

arrow::Status HTTPRandomAccessFile::Seek(int64_t position) {
  return toArrowStatus(impl_->Seek(position));
}

arrow::Result<int64_t> HTTPRandomAccessFile::ReadAt(int64_t position,
                                                    int64_t nbytes,
                                                    void* out) {
  if (impl_->Closed()) {
    return arrow::Status::Invalid("File is closed");
  }
  int64_t bytes_read = 0;
  auto status = impl_->readAt(position, nbytes, out, &bytes_read);
  if (!status.ok()) {
    return toArrowStatus(status);
  }
  return bytes_read;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> HTTPRandomAccessFile::ReadAt(
    int64_t position, int64_t nbytes) {
  if (impl_->Closed()) {
    return arrow::Status::Invalid("File is closed");
  }
  if (nbytes == 0) {
    return std::make_shared<arrow::Buffer>(nullptr, 0);
  }

  auto buffer_result = arrow::AllocateBuffer(nbytes);
  if (!buffer_result.ok()) {
    return buffer_result.status();
  }

  auto buffer = std::move(buffer_result).ValueOrDie();
  int64_t bytes_read = 0;
  auto status =
      impl_->readAt(position, nbytes, buffer->mutable_data(), &bytes_read);
  if (!status.ok()) {
    return toArrowStatus(status);
  }

  std::shared_ptr<arrow::Buffer> shared_buffer = std::move(buffer);
  return arrow::SliceBuffer(shared_buffer, 0, bytes_read);
}

arrow::Result<int64_t> HTTPRandomAccessFile::Read(int64_t nbytes, void* out) {
  int64_t bytes_read = 0;
  auto status = impl_->Read(nbytes, out, &bytes_read);
  if (!status.ok()) {
    return toArrowStatus(status);
  }
  return bytes_read;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> HTTPRandomAccessFile::Read(
    int64_t nbytes) {
  if (nbytes == 0) {
    return std::make_shared<arrow::Buffer>(nullptr, 0);
  }
  auto buffer_result = arrow::AllocateBuffer(nbytes);
  if (!buffer_result.ok()) {
    return buffer_result.status();
  }
  auto buffer = std::move(buffer_result).ValueOrDie();
  int64_t bytes_read = 0;
  auto status =
      impl_->Read(nbytes, buffer->mutable_data(), &bytes_read);
  if (!status.ok()) {
    return toArrowStatus(status);
  }
  std::shared_ptr<arrow::Buffer> shared_buffer = std::move(buffer);
  return arrow::SliceBuffer(shared_buffer, 0, bytes_read);
}

arrow::Status HTTPRandomAccessFile::Close() {
  return toArrowStatus(impl_->Close());
}

bool HTTPRandomAccessFile::closed() const { return impl_->Closed(); }

// ============================================================================
// HTTPFileSystem Implementation
// ============================================================================

HTTPFileSystem::HTTPFileSystem(const common::case_insensitive_map_t<std::string>& options)
    : options_(options) {
  ensureHttpCurlGlobalInit();
}

HTTPFileSystem::HTTPFileSystem(const reader::FileSchema& schema)
    : HTTPFileSystem(schema.options) {
  // Validate all paths are HTTP(S) URLs
  for (const auto& path : schema.paths) {
    try {
      HTTPURIComponents::parse(path);
    } catch (const exception::Exception& e) {
      THROW_IO_EXCEPTION("Invalid HTTP URL: " + path + " - " + e.what());
    }
  }
}

HTTPFileSystem::~HTTPFileSystem() {
  // Note: We don't call curl_global_cleanup() because it's shared
  // across all HTTPFileSystem instances
}

bool HTTPFileSystem::Equals(const arrow::fs::FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  // For simplicity, consider all HTTP filesystems equal
  // In a full implementation, you might compare options
  return true;
}

arrow::Result<arrow::fs::FileInfo> HTTPFileSystem::GetFileInfo(
    const std::string& path) {
  // Validate path is HTTP(S) URL
  try {
    auto components = HTTPURIComponents::parse(path);
  } catch (const exception::Exception& e) {
    return arrow::Status::Invalid("Invalid HTTP URL: " + path);
  }

  // Lightweight HEAD-only probe: send a HEAD request directly with a
  // temporary CURL handle instead of creating a full HTTPRandomAccessFile.
  CURL* curl = curl_easy_init();
  if (!curl) {
    return arrow::Status::IOError("Failed to create CURL handle for HEAD request");
  }

  // Minimal setup — follow redirects and disable SSL verification only if
  // the options say so (defaults to verifying).
  curl_easy_setopt(curl, CURLOPT_URL, path.c_str());
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);  // HEAD request
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    curl_easy_cleanup(curl);
    arrow::fs::FileInfo info;
    info.set_path(path);
    info.set_type(arrow::fs::FileType::NotFound);
    return info;
  }

  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  if (http_code < 200 || http_code >= 300) {
    curl_easy_cleanup(curl);
    arrow::fs::FileInfo info;
    info.set_path(path);
    info.set_type(arrow::fs::FileType::NotFound);
    return info;
  }

  double content_length = -1;
  curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
  curl_easy_cleanup(curl);

  arrow::fs::FileInfo info;
  info.set_path(path);
  info.set_type(arrow::fs::FileType::File);
  info.set_size(content_length >= 0 ? static_cast<int64_t>(content_length) : -1);
  return info;
}

arrow::Result<std::vector<arrow::fs::FileInfo>> HTTPFileSystem::GetFileInfo(
    const arrow::fs::FileSelector& selector) {
  // HTTP doesn't support directory listing
  return arrow::Status::NotImplemented(
      "Directory listing not supported for HTTP filesystem");
}

arrow::Status HTTPFileSystem::CreateDir(const std::string& path, bool recursive) {
  return arrow::Status::NotImplemented(
      "CreateDir not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::DeleteDir(const std::string& path) {
  return arrow::Status::NotImplemented(
      "DeleteDir not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::DeleteDirContents(const std::string& path,
                                                 bool missing_dir_ok) {
  return arrow::Status::NotImplemented(
      "DeleteDirContents not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::DeleteRootDirContents() {
  return arrow::Status::NotImplemented(
      "DeleteRootDirContents not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::DeleteFile(const std::string& path) {
  return arrow::Status::NotImplemented(
      "DeleteFile not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::Move(const std::string& src, 
                                    const std::string& dest) {
  return arrow::Status::NotImplemented(
      "Move not supported for HTTP filesystem (read-only)");
}

arrow::Status HTTPFileSystem::CopyFile(const std::string& src,
                                        const std::string& dest) {
  return arrow::Status::NotImplemented(
      "CopyFile not supported for HTTP filesystem (read-only)");
}

arrow::Result<std::shared_ptr<arrow::io::InputStream>> 
HTTPFileSystem::OpenInputStream(const std::string& path) {
  // For now, just return the RandomAccessFile (which is also an InputStream)
  return OpenInputFile(path);
}

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> 
HTTPFileSystem::OpenInputFile(const std::string& path) {
  try {
    auto file = std::make_shared<HTTPRandomAccessFile>(path, options_);
    return file;
  } catch (const exception::Exception& e) {
    return arrow::Status::IOError("Failed to open HTTP file: " + 
                                   std::string(e.what()));
  } catch (const std::exception& e) {
    return arrow::Status::IOError("Failed to open HTTP file (unexpected error): " + 
                                   std::string(e.what()));
  }
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
HTTPFileSystem::OpenOutputStream(
    const std::string& path,
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
  return arrow::Status::NotImplemented(
      "OpenOutputStream not supported for HTTP filesystem (read-only)");
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
HTTPFileSystem::OpenAppendStream(
    const std::string& path,
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
  return arrow::Status::NotImplemented(
      "OpenAppendStream not supported for HTTP filesystem (read-only)");
}

// --- neug::fsys::FileSystem interface ---

std::vector<std::string> HTTPFileSystem::glob(const std::string& path) {
  // HTTP has no directory listing or glob expansion; return path unchanged.
  return {path};
}

std::unique_ptr<io::RandomAccessFile> HTTPFileSystem::openInputFile(
    const std::string& path) {
  return std::make_unique<HttpRandomAccessFile>(path, options_);
}

std::shared_ptr<void> HTTPFileSystem::getArrowFileSystem() {
  return std::static_pointer_cast<void>(
      std::shared_ptr<arrow::fs::FileSystem>(
          std::make_shared<HTTPFileSystem>(options_)));
}

std::unique_ptr<fsys::FileSystem> CreateHTTPFileSystem(
    const reader::FileSchema& schema) {
  return std::make_unique<HTTPFileSystem>(schema);
}

}  // namespace http
}  // namespace extension
}  // namespace neug
