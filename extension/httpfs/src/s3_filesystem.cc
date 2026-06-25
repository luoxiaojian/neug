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

#include "s3_filesystem.h"

#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW
#include "s3_options.h"
#endif
#include "s3_random_access_file.h"

#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW
#include <arrow/filesystem/api.h>
#include <arrow/result.h>
#endif
#include <glog/logging.h>
#include <algorithm>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/vfs/file_system.h"

namespace {

std::string stripS3Scheme(const std::string& path) {
  if (path.size() > 5 && path.substr(0, 5) == "s3://") {
    return path.substr(5);
  }
  if (path.size() > 6 && path.substr(0, 6) == "oss://") {
    return path.substr(6);
  }
  return path;
}

}  // anonymous namespace

#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW
namespace {

class S3FileSystemWrapper : public arrow::fs::FileSystem {
 public:
  explicit S3FileSystemWrapper(std::shared_ptr<arrow::fs::S3FileSystem> inner)
      : inner_(std::move(inner)) {}

  std::string type_name() const override { return inner_->type_name(); }

  bool Equals(const arrow::fs::FileSystem& other) const override {
    return inner_->Equals(other);
  }

  arrow::Result<std::string> NormalizePath(std::string path) override {
    return inner_->NormalizePath(std::move(path));
  }

  arrow::Result<std::string> PathFromUri(
      const std::string& uri_string) const override {
    return inner_->PathFromUri(uri_string);
  }

  arrow::Result<std::string> MakeUri(std::string path) const override {
    return inner_->MakeUri(std::move(path));
  }

  arrow::Result<arrow::fs::FileInfo> GetFileInfo(
      const std::string& path) override {
    return inner_->GetFileInfo(stripS3Scheme(path));
  }
  arrow::Result<std::vector<arrow::fs::FileInfo>> GetFileInfo(
      const arrow::fs::FileSelector& selector) override {
    return inner_->GetFileInfo(selector);
  }
  arrow::Result<std::vector<arrow::fs::FileInfo>> GetFileInfo(
      const std::vector<std::string>& paths) override {
    std::vector<std::string> stripped;
    stripped.reserve(paths.size());
    for (const auto& p : paths) {
      stripped.push_back(stripS3Scheme(p));
    }
    return inner_->GetFileInfo(stripped);
  }

  arrow::Status CreateDir(const std::string& path, bool recursive) override {
    return inner_->CreateDir(stripS3Scheme(path), recursive);
  }
  arrow::Status DeleteDir(const std::string& path) override {
    return inner_->DeleteDir(stripS3Scheme(path));
  }
  arrow::Status DeleteDirContents(const std::string& path,
                                  bool missing_dir_ok) override {
    return inner_->DeleteDirContents(stripS3Scheme(path), missing_dir_ok);
  }
  arrow::Status DeleteRootDirContents() override {
    return inner_->DeleteRootDirContents();
  }
  arrow::Status DeleteFile(const std::string& path) override {
    return inner_->DeleteFile(stripS3Scheme(path));
  }
  arrow::Status DeleteFiles(const std::vector<std::string>& paths) override {
    std::vector<std::string> stripped;
    stripped.reserve(paths.size());
    for (const auto& p : paths) {
      stripped.push_back(stripS3Scheme(p));
    }
    return inner_->DeleteFiles(stripped);
  }
  arrow::Status Move(const std::string& src, const std::string& dest) override {
    return inner_->Move(stripS3Scheme(src), stripS3Scheme(dest));
  }
  arrow::Status CopyFile(const std::string& src,
                         const std::string& dest) override {
    return inner_->CopyFile(stripS3Scheme(src), stripS3Scheme(dest));
  }

  arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
      const std::string& path) override {
    return inner_->OpenInputStream(stripS3Scheme(path));
  }
  arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
      const arrow::fs::FileInfo& info) override {
    return inner_->OpenInputStream(info);
  }

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override {
    return inner_->OpenInputFile(stripS3Scheme(path));
  }
  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
      const arrow::fs::FileInfo& info) override {
    return inner_->OpenInputFile(info);
  }

  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
      override {
    return inner_->OpenOutputStream(stripS3Scheme(path), metadata);
  }
  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
      override {
    return inner_->OpenAppendStream(stripS3Scheme(path), metadata);
  }

 private:
  std::shared_ptr<arrow::fs::S3FileSystem> inner_;
};

}  // anonymous namespace
#endif  // NEUG_USE_ARROW

namespace neug {
namespace extension {
namespace s3 {

S3URIComponents S3URIComponents::parse(const std::string& uri) {
  S3URIComponents components;
  std::string path = stripS3Scheme(uri);

  if (path == uri && uri.find("://") != std::string::npos) {
    THROW_IO_EXCEPTION(
        "Invalid S3 URI: expected s3:// or oss:// scheme, got: " + uri);
  }

  size_t slash_pos = path.find('/');
  if (slash_pos == std::string::npos) {
    components.bucket = path;
    components.objectKey = "";
  } else {
    components.bucket = path.substr(0, slash_pos);
    components.objectKey = path.substr(slash_pos + 1);
  }

  if (components.bucket.empty()) {
    THROW_IO_EXCEPTION("Invalid S3 URI: missing bucket name in " + uri);
  }
  if (components.bucket.length() < 3 || components.bucket.length() > 63) {
    THROW_IO_EXCEPTION("Invalid S3 bucket name: length must be 3-63 characters, got: " +
                       components.bucket);
  }

  components.hasGlob = (components.objectKey.find('*') != std::string::npos ||
                        components.objectKey.find('?') != std::string::npos ||
                        components.objectKey.find('[') != std::string::npos);
  return components;
}

S3FileSystem::S3FileSystem(const reader::FileSchema& schema)
    : config_(buildS3ClientConfig(schema)),
      client_(std::make_shared<S3Client>(config_)),
      schema_(schema) {
  if (schema.paths.empty()) {
    THROW_IO_EXCEPTION("S3FileSystem: no paths provided");
  }
  LOG(INFO) << "S3FileSystem initialized (native IO client)";
}

std::pair<std::string, std::string> S3FileSystem::parseBucketKeyPath(
    const std::string& path) {
  auto components = S3URIComponents::parse(path);
  if (components.objectKey.empty()) {
    THROW_IO_EXCEPTION("S3 path must include an object key: " + path);
  }
  return {components.bucket, components.objectKey};
}

std::vector<std::string> S3FileSystem::glob(const std::string& path) {
  auto components = S3URIComponents::parse(path);

  std::string normalized_path = components.bucket;
  if (!components.objectKey.empty()) {
    normalized_path += "/" + components.objectKey;
  }

  if (!components.hasGlob) {
    LOG(INFO) << "Direct S3 path: " << normalized_path;
    return {normalized_path};
  }

  LOG(INFO) << "Expanding S3 glob pattern: " << path;
  std::vector<std::string> out_paths;
  auto list_keys = [this, bucket = components.bucket](
                       const std::string& list_prefix) {
    return client_->listObjectKeys(bucket, list_prefix);
  };
  ResolvePathsWithGlobOnKeys(list_keys, components.bucket, components.objectKey,
                             out_paths, path);
  return out_paths;
}

std::unique_ptr<io::RandomAccessFile> S3FileSystem::openInputFile(
    const std::string& path) {
  auto [bucket, key] = parseBucketKeyPath(path);
  return std::make_unique<S3RandomAccessFile>(client_, bucket, key);
}

#if defined(NEUG_USE_ARROW) && NEUG_USE_ARROW
void S3FileSystem::ensureArrowFileSystem() const {
  std::lock_guard<std::mutex> lock(arrow_mtx_);
  if (arrow_fs_) {
    return;
  }
  auto init_result = arrow::fs::EnsureS3Initialized();
  if (!init_result.ok()) {
    THROW_IO_EXCEPTION("Failed to initialize Arrow S3 subsystem: " +
                       init_result.ToString());
  }
  auto s3_options = buildS3Options(schema_);
  auto fs_result = arrow::fs::S3FileSystem::Make(s3_options);
  if (!fs_result.ok()) {
    THROW_IO_EXCEPTION("Failed to initialize Arrow S3FileSystem: " +
                       fs_result.status().ToString());
  }
  arrow_fs_ = *fs_result;
}

std::shared_ptr<void> S3FileSystem::getArrowFileSystem() const {
  ensureArrowFileSystem();
  return std::static_pointer_cast<void>(
      std::shared_ptr<arrow::fs::FileSystem>(
          std::make_shared<S3FileSystemWrapper>(arrow_fs_)));
}

arrow::fs::S3Options S3FileSystem::buildS3Options(
    const reader::FileSchema& schema) {
  S3OptionsBuilder builder(schema);
  return builder.build();
}

std::vector<std::string> S3FileSystem::resolveS3Paths(
    std::shared_ptr<arrow::fs::S3FileSystem> fs,
    const std::vector<std::string>& paths) {
  std::vector<std::string> resolved_paths;

  for (const auto& path : paths) {
    auto components = S3URIComponents::parse(path);
    std::string arrow_path = components.bucket;
    if (!components.objectKey.empty()) {
      arrow_path += "/" + components.objectKey;
    }

    if (!components.hasGlob) {
      resolved_paths.push_back(arrow_path);
      LOG(INFO) << "Direct S3 path: " << arrow_path;
    } else {
      LOG(INFO) << "Expanding S3 glob pattern: " << path;
      ResolvePathsWithGlobOnFs(fs, components.bucket, components.objectKey,
                               resolved_paths, path);
    }
  }

  std::sort(resolved_paths.begin(), resolved_paths.end());
  return resolved_paths;
}
#endif  // NEUG_USE_ARROW

std::unique_ptr<fsys::FileSystem> CreateS3FileSystem(
    const reader::FileSchema& schema) {
  return std::make_unique<S3FileSystem>(schema);
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
