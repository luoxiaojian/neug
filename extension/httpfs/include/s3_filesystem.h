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

#pragma once

#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#endif
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "neug/utils/io/vfs/file_system.h"
#include "neug/utils/io/read/common/schema.h"
#include "glob_utils.h"
#include "s3_client.h"
#include "s3_client_config.h"

namespace neug {
namespace extension {
namespace s3 {

struct S3URIComponents {
  std::string bucket;
  std::string objectKey;
  bool hasGlob;

  static S3URIComponents parse(const std::string& uri);
};

class S3FileSystem : public fsys::FileSystem {
 public:
  explicit S3FileSystem(const reader::FileSchema& schema);

  std::vector<std::string> glob(const std::string& path) override;

  std::unique_ptr<io::RandomAccessFile> openInputFile(
      const std::string& path) override;

#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW
  std::shared_ptr<void> getArrowFileSystem() const override;

  static arrow::fs::S3Options buildS3Options(const reader::FileSchema& schema);

  std::vector<std::string> resolveS3Paths(
      std::shared_ptr<arrow::fs::S3FileSystem> fs,
      const std::vector<std::string>& paths);
#else
  std::shared_ptr<void> getArrowFileSystem() const override { return nullptr; }
#endif

 private:
#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW
  void ensureArrowFileSystem() const;
#endif

  static std::pair<std::string, std::string> parseBucketKeyPath(
      const std::string& path);

  S3ClientConfig config_;
  std::shared_ptr<S3Client> client_;
#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW
  mutable std::mutex arrow_mtx_;
  mutable std::shared_ptr<arrow::fs::S3FileSystem> arrow_fs_;
#endif
  reader::FileSchema schema_;
};

std::unique_ptr<fsys::FileSystem> CreateS3FileSystem(
    const reader::FileSchema& schema);

}  // namespace s3
}  // namespace extension
}  // namespace neug
