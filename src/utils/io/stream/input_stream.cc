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

#include "neug/utils/io/stream/input_stream.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <mutex>
#include <streambuf>
#include <vector>

#include <glog/logging.h>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace io {
namespace {

std::string normalizeLocalPath(const std::string& path) {
  constexpr const char* kFilePrefix = "file://";
  if (path.starts_with(kFilePrefix)) {
    std::string local_path = path.substr(strlen(kFilePrefix));
    if (local_path.empty() || local_path[0] != '/') {
      local_path = "/" + local_path;
    }
    return local_path;
  }
  return path;
}

class LocalRandomAccessFile : public RandomAccessFile {
 public:
  explicit LocalRandomAccessFile(const std::string& path)
      : path_(path),
        stream_(path, std::ios::binary | std::ios::in) {
    if (!stream_) {
      THROW_IO_EXCEPTION("Failed to open input file: " + path);
    }
    stream_.seekg(0, std::ios::end);
    size_ = stream_.tellg();
    stream_.seekg(0, std::ios::beg);
    position_ = 0;
  }

  int64_t Tell() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return position_;
  }

  int64_t Size() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return size_;
  }

  neug::Status Seek(int64_t position) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      return neug::Status(StatusCode::ERR_IO_ERROR, "File is closed");
    }
    if (position < 0) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Negative seek position");
    }
    stream_.clear();
    stream_.seekg(position, std::ios::beg);
    if (!stream_) {
      return neug::Status(StatusCode::ERR_IO_ERROR,
                          "Failed to seek file: " + path_);
    }
    position_ = position;
    return neug::Status::OK();
  }

  neug::Status Read(int64_t nbytes, void* out, int64_t* bytes_read) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (bytes_read == nullptr) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "bytes_read output is null");
    }
    *bytes_read = 0;
    if (closed_) {
      return neug::Status(StatusCode::ERR_IO_ERROR, "File is closed");
    }
    if (nbytes <= 0) {
      return neug::Status::OK();
    }
    stream_.clear();
    stream_.read(static_cast<char*>(out), static_cast<std::streamsize>(nbytes));
    *bytes_read = stream_.gcount();
    position_ += *bytes_read;
    if (*bytes_read == 0 && !stream_.eof()) {
      return neug::Status(StatusCode::ERR_IO_ERROR,
                          "Failed to read file: " + path_);
    }
    return neug::Status::OK();
  }

  neug::Status ReadAt(int64_t position, int64_t nbytes, void* out,
                      int64_t* bytes_read) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (bytes_read == nullptr) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "bytes_read output is null");
    }
    *bytes_read = 0;
    if (closed_) {
      return neug::Status(StatusCode::ERR_IO_ERROR, "File is closed");
    }
    if (nbytes <= 0) {
      position_ = position;
      return neug::Status::OK();
    }
    if (position < 0) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Negative read position");
    }
    stream_.clear();
    stream_.seekg(position, std::ios::beg);
    if (!stream_) {
      return neug::Status(StatusCode::ERR_IO_ERROR,
                          "Failed to seek file: " + path_);
    }
    stream_.read(static_cast<char*>(out), static_cast<std::streamsize>(nbytes));
    *bytes_read = stream_.gcount();
    position_ = position + *bytes_read;
    if (*bytes_read == 0 && !stream_.eof()) {
      return neug::Status(StatusCode::ERR_IO_ERROR,
                          "Failed to read file: " + path_);
    }
    return neug::Status::OK();
  }

  neug::Status Close() override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!closed_) {
      stream_.close();
      closed_ = true;
    }
    return neug::Status::OK();
  }

  bool Closed() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
  }

 private:
  std::string path_;
  std::ifstream stream_;
  int64_t size_ = -1;
  int64_t position_ = 0;
  bool closed_ = false;
  mutable std::mutex mutex_;
};

class RandomAccessStreambuf : public std::streambuf {
 public:
  explicit RandomAccessStreambuf(std::unique_ptr<RandomAccessFile> file)
      : file_(std::move(file)) {
    buffer_.resize(kBufferSize);
    setg(buffer_.data(), buffer_.data(), buffer_.data());
  }

 protected:
  int underflow() override {
    if (gptr() < egptr()) {
      return traits_type::to_int_type(*gptr());
    }
    if (file_ == nullptr || file_->Closed()) {
      return traits_type::eof();
    }
    int64_t bytes_read = 0;
    auto status = file_->Read(static_cast<int64_t>(buffer_.size()),
                              buffer_.data(), &bytes_read);
    if (!status.ok()) {
      LOG(WARNING) << "RandomAccessStreambuf::underflow: Read failed: "
                   << status.ToString();
      return traits_type::eof();
    }
    if (bytes_read <= 0) {
      return traits_type::eof();
    }
    setg(buffer_.data(), buffer_.data(), buffer_.data() + bytes_read);
    return traits_type::to_int_type(*gptr());
  }

  std::streampos seekoff(std::streamoff off, std::ios_base::seekdir way,
                         std::ios_base::openmode which) override {
    if (file_ == nullptr || file_->Closed()) {
      return std::streampos(std::streamoff(-1));
    }
    if ((which & std::ios_base::out) != 0) {
      return std::streampos(std::streamoff(-1));
    }

    int64_t base = file_->Tell();
    if (way == std::ios_base::cur && gptr() != egptr()) {
      base += static_cast<int64_t>(gptr() - egptr());
    } else if (way == std::ios_base::end) {
      base = file_->Size();
    } else if (way == std::ios_base::beg) {
      base = 0;
    }

    int64_t target = base + static_cast<int64_t>(off);
    if (file_->Seek(target).ok()) {
      setg(buffer_.data(), buffer_.data(), buffer_.data());
      return std::streampos(static_cast<std::streamoff>(target));
    }
    return std::streampos(std::streamoff(-1));
  }

  std::streampos seekpos(std::streampos pos,
                         std::ios_base::openmode which) override {
    return seekoff(static_cast<std::streamoff>(pos), std::ios_base::beg, which);
  }

 private:
  static constexpr size_t kBufferSize = 64 * 1024;
  std::unique_ptr<RandomAccessFile> file_;
  std::vector<char> buffer_;
};

class RandomAccessIstream : public std::istream {
 public:
  explicit RandomAccessIstream(std::unique_ptr<RandomAccessFile> file)
      : std::istream(nullptr),
        streambuf_(std::make_unique<RandomAccessStreambuf>(std::move(file))) {
    init(streambuf_.get());
  }

 private:
  std::unique_ptr<RandomAccessStreambuf> streambuf_;
};

}  // namespace

std::unique_ptr<RandomAccessFile> openLocalInputFile(const std::string& path) {
  return std::make_unique<LocalRandomAccessFile>(normalizeLocalPath(path));
}

std::unique_ptr<std::istream> randomAccessToIstream(
    std::unique_ptr<RandomAccessFile> file) {
  return std::make_unique<RandomAccessIstream>(std::move(file));
}

}  // namespace io
}  // namespace neug
