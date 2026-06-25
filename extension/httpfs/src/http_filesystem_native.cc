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

// Native-only HTTPFileSystem implementation (compiled when USE_ARROW=OFF).
// When Arrow is enabled, the full implementation in http_filesystem.cc is used.
#if !defined(NEUG_USE_ARROW) || !NEUG_USE_ARROW

#include "http_filesystem.h"
#include "http_random_access_file.h"

#include <glog/logging.h>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/vfs/file_system.h"

namespace neug {
namespace extension {
namespace http {

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
  std::string url = scheme + "://" + host;
  if ((scheme == "http" && port != 80) ||
      (scheme == "https" && port != 443)) {
    url += ":" + std::to_string(port);
  }
  url += path;
  return url;
}

// ============================================================================
// HTTPFileSystem (native-only)
// ============================================================================

HTTPFileSystem::HTTPFileSystem(
    const common::case_insensitive_map_t<std::string>& options)
    : options_(options) {
  ensureHttpCurlGlobalInit();
}

HTTPFileSystem::HTTPFileSystem(const reader::FileSchema& schema)
    : HTTPFileSystem(schema.options) {
  for (const auto& path : schema.paths) {
    try {
      HTTPURIComponents::parse(path);
    } catch (const exception::Exception& e) {
      THROW_IO_EXCEPTION("Invalid HTTP URL: " + path + " - " + e.what());
    }
  }
}

HTTPFileSystem::~HTTPFileSystem() = default;

std::vector<std::string> HTTPFileSystem::glob(const std::string& path) {
  // HTTP has no directory listing or glob expansion; return path unchanged.
  return {path};
}

std::unique_ptr<io::RandomAccessFile> HTTPFileSystem::openInputFile(
    const std::string& path) {
  return std::make_unique<HttpRandomAccessFile>(path, options_);
}

std::unique_ptr<fsys::FileSystem> CreateHTTPFileSystem(
    const reader::FileSchema& schema) {
  return std::make_unique<HTTPFileSystem>(schema);
}

}  // namespace http
}  // namespace extension
}  // namespace neug

#endif  // !NEUG_USE_ARROW
