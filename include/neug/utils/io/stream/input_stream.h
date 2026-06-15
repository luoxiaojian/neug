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
#pragma once

#include <cstdint>
#include <istream>
#include <memory>
#include <string>

#include "neug/utils/result.h"

namespace neug {
namespace io {

/// Random-access byte source for remote and local file reads.
class RandomAccessFile {
 public:
  virtual ~RandomAccessFile() = default;

  virtual int64_t Tell() const = 0;
  virtual int64_t Size() = 0;
  virtual neug::Status Seek(int64_t position) = 0;
  virtual neug::Status Read(int64_t nbytes, void* out, int64_t* bytes_read) = 0;
  /// Random read at an absolute offset. Default uses Seek+Read; implementations
  /// that may be accessed concurrently should override with a single lock.
  virtual neug::Status ReadAt(int64_t position, int64_t nbytes, void* out,
                              int64_t* bytes_read) {
    auto status = Seek(position);
    if (!status.ok()) {
      return status;
    }
    return Read(nbytes, out, bytes_read);
  }
  virtual neug::Status Close() = 0;
  virtual bool Closed() const = 0;
};

/// Opens a local file for reading (strips optional file:// prefix).
std::unique_ptr<RandomAccessFile> openLocalInputFile(const std::string& path);

/// Wraps a RandomAccessFile as a sequential std::istream (for csv-parser).
std::unique_ptr<std::istream> randomAccessToIstream(
    std::unique_ptr<RandomAccessFile> file);

}  // namespace io
}  // namespace neug
