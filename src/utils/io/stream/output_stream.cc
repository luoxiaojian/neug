/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/utils/io/stream/output_stream.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <string>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"

namespace neug {
namespace io {
namespace {

class FileOutputStream : public OutputStream {
 public:
  explicit FileOutputStream(const std::string& path)
      : stream_(path, std::ios::binary | std::ios::trunc) {
    if (!stream_) {
      if (errno == EACCES || errno == EPERM) {
        THROW_PERMISSION_DENIED("Failed to open output file: " + path);
      }
      THROW_IO_EXCEPTION("Failed to open output file: " + path);
    }
  }

  neug::Status Write(const uint8_t* data, int64_t nbytes) override {
    if (nbytes <= 0) {
      return neug::Status::OK();
    }
    stream_.write(reinterpret_cast<const char*>(data),
                  static_cast<std::streamsize>(nbytes));
    if (!stream_) {
      return neug::Status(
          StatusCode::ERR_IO_ERROR,
          std::string("Failed to write file: ") + std::strerror(errno));
    }
    return neug::Status::OK();
  }

  neug::Status Close() override {
    stream_.close();
    if (stream_.fail()) {
      return neug::Status(
          StatusCode::ERR_IO_ERROR,
          std::string("Failed to close file: ") + std::strerror(errno));
    }
    return neug::Status::OK();
  }

 private:
  std::ofstream stream_;
};

}  // namespace

std::unique_ptr<OutputStream> openLocalOutputStream(const std::string& path) {
  return std::make_unique<FileOutputStream>(normalizeLocalPath(path));
}

}  // namespace io
}  // namespace neug
