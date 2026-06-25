/**
 * Native-mode unit tests for httpfs extension (no Arrow dependency).
 *
 * Covers: S3URIComponents, HTTPURIComponents, MatchGlobPattern,
 * S3ClientConfig, S3ConfigOptionKeys, HTTPFileSystem (native),
 * S3FileSystem (native).
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "glob_utils.h"
#include "http_filesystem.h"
#include "s3_client_config.h"
#include "s3_config_keys.h"
#include "s3_filesystem.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/schema.h"

using neug::extension::http::CreateHTTPFileSystem;
using neug::extension::http::HTTPFileSystem;
using neug::extension::http::HTTPURIComponents;
using neug::extension::s3::buildS3ClientConfig;
using neug::extension::s3::CreateS3FileSystem;
using neug::extension::s3::MatchGlobPattern;
using neug::extension::s3::S3ClientConfig;
using neug::extension::s3::S3ConfigOptionKeys;
using neug::extension::s3::S3CredentialsKindValues;
using neug::extension::s3::S3FileSystem;
using neug::extension::s3::S3URIComponents;
using neug::reader::FileSchema;

// ============================================================================
// S3URIComponents Tests
// ============================================================================

TEST(S3URIComponentsTest, ParseSimpleBucketKey) {
  auto c = S3URIComponents::parse("s3://my-bucket/path/to/file.parquet");
  EXPECT_EQ(c.bucket, "my-bucket");
  EXPECT_EQ(c.objectKey, "path/to/file.parquet");
  EXPECT_FALSE(c.hasGlob);
}

TEST(S3URIComponentsTest, ParseOSSScheme) {
  auto c = S3URIComponents::parse("oss://test-bucket/data.csv");
  EXPECT_EQ(c.bucket, "test-bucket");
  EXPECT_EQ(c.objectKey, "data.csv");
  EXPECT_FALSE(c.hasGlob);
}

TEST(S3URIComponentsTest, ParseBucketOnly) {
  auto c = S3URIComponents::parse("s3://my-bucket");
  EXPECT_EQ(c.bucket, "my-bucket");
  EXPECT_EQ(c.objectKey, "");
  EXPECT_FALSE(c.hasGlob);
}

TEST(S3URIComponentsTest, ParseGlobStar) {
  auto c = S3URIComponents::parse("s3://bucket/prefix/*.parquet");
  EXPECT_EQ(c.bucket, "bucket");
  EXPECT_EQ(c.objectKey, "prefix/*.parquet");
  EXPECT_TRUE(c.hasGlob);
}

TEST(S3URIComponentsTest, ParseGlobQuestion) {
  auto c = S3URIComponents::parse("s3://bucket/data?.csv");
  EXPECT_TRUE(c.hasGlob);
}

TEST(S3URIComponentsTest, ParseGlobBracket) {
  auto c = S3URIComponents::parse("s3://bucket/file[0-9].csv");
  EXPECT_TRUE(c.hasGlob);
}

TEST(S3URIComponentsTest, InvalidSchemeThrows) {
  EXPECT_THROW(S3URIComponents::parse("http://bucket/key"),
               neug::exception::Exception);
}

TEST(S3URIComponentsTest, EmptyBucketThrows) {
  EXPECT_THROW(S3URIComponents::parse("s3:///key"),
               neug::exception::Exception);
}

TEST(S3URIComponentsTest, ShortBucketNameThrows) {
  EXPECT_THROW(S3URIComponents::parse("s3://ab/key"),
               neug::exception::Exception);
}

// ============================================================================
// HTTPURIComponents Tests
// ============================================================================

TEST(HTTPURIComponentsTest, ParseSimpleHTTPS) {
  auto c = HTTPURIComponents::parse("https://example.com/path/file.parquet");
  EXPECT_EQ(c.scheme, "https");
  EXPECT_EQ(c.host, "example.com");
  EXPECT_EQ(c.port, 443);
  EXPECT_EQ(c.path, "/path/file.parquet");
}

TEST(HTTPURIComponentsTest, ParseHTTPWithPort) {
  auto c = HTTPURIComponents::parse("http://localhost:8080/data/file.csv");
  EXPECT_EQ(c.scheme, "http");
  EXPECT_EQ(c.host, "localhost");
  EXPECT_EQ(c.port, 8080);
  EXPECT_EQ(c.path, "/data/file.csv");
}

TEST(HTTPURIComponentsTest, ParseHTTPDefaultPort) {
  auto c = HTTPURIComponents::parse("http://example.com/file");
  EXPECT_EQ(c.port, 80);
}

TEST(HTTPURIComponentsTest, ParseNoPath) {
  auto c = HTTPURIComponents::parse("https://example.com");
  EXPECT_EQ(c.host, "example.com");
  EXPECT_EQ(c.path, "/");
}

TEST(HTTPURIComponentsTest, ToURLDefault) {
  HTTPURIComponents c;
  c.scheme = "https";
  c.host = "example.com";
  c.port = 443;
  c.path = "/data.csv";
  EXPECT_EQ(c.toURL(), "https://example.com/data.csv");
}

TEST(HTTPURIComponentsTest, ToURLNonDefaultPort) {
  HTTPURIComponents c;
  c.scheme = "http";
  c.host = "localhost";
  c.port = 9000;
  c.path = "/file";
  EXPECT_EQ(c.toURL(), "http://localhost:9000/file");
}

TEST(HTTPURIComponentsTest, InvalidSchemeThrows) {
  EXPECT_THROW(HTTPURIComponents::parse("ftp://example.com/file"),
               neug::exception::Exception);
}

TEST(HTTPURIComponentsTest, MissingSchemeThrows) {
  EXPECT_THROW(HTTPURIComponents::parse("example.com/file"),
               neug::exception::Exception);
}

// ============================================================================
// MatchGlobPattern Tests
// ============================================================================

TEST(MatchGlobPatternTest, StarMatchesAny) {
  EXPECT_TRUE(MatchGlobPattern("data/file.parquet", "data/*.parquet"));
  EXPECT_TRUE(MatchGlobPattern("data/sub/file.parquet", "data/*.parquet"));
}

TEST(MatchGlobPatternTest, QuestionMatchesSingleChar) {
  EXPECT_TRUE(MatchGlobPattern("file1.csv", "file?.csv"));
  EXPECT_FALSE(MatchGlobPattern("file12.csv", "file?.csv"));
}

TEST(MatchGlobPatternTest, CharacterClass) {
  EXPECT_TRUE(MatchGlobPattern("file1.csv", "file[0-9].csv"));
  EXPECT_FALSE(MatchGlobPattern("filea.csv", "file[0-9].csv"));
}

TEST(MatchGlobPatternTest, ExactMatch) {
  EXPECT_TRUE(MatchGlobPattern("exact.csv", "exact.csv"));
  EXPECT_FALSE(MatchGlobPattern("other.csv", "exact.csv"));
}

TEST(MatchGlobPatternTest, StarMatchesEmpty) {
  EXPECT_TRUE(MatchGlobPattern("prefix.csv", "prefix*"));
  EXPECT_TRUE(MatchGlobPattern("prefix_extra.csv", "prefix*"));
}

// ============================================================================
// S3ConfigOptionKeys Tests
// ============================================================================

TEST(S3ConfigKeysTest, ConstantsAreCorrect) {
  EXPECT_STREQ(S3ConfigOptionKeys::kEndpointCanonical, "OSS_ENDPOINT");
  EXPECT_STREQ(S3ConfigOptionKeys::kEndpointAws, "AWS_ENDPOINT_URL");
  EXPECT_STREQ(S3ConfigOptionKeys::kRegionCanonical, "OSS_REGION");
  EXPECT_STREQ(S3ConfigOptionKeys::kAccessKeyCanonical, "OSS_ACCESS_KEY_ID");
  EXPECT_STREQ(S3ConfigOptionKeys::kSecretAccessKeyCanonical,
               "OSS_ACCESS_KEY_SECRET");
}

TEST(S3ConfigKeysTest, CredentialsKindValues) {
  EXPECT_STREQ(S3CredentialsKindValues::kExplicit, "explicit");
  EXPECT_STREQ(S3CredentialsKindValues::kAnonymous, "anonymous");
  EXPECT_STREQ(S3CredentialsKindValues::kDefault, "default");
}

// ============================================================================
// S3ClientConfig Tests
// ============================================================================

TEST(S3ClientConfigTest, BuildWithExplicitCredentials) {
  FileSchema schema;
  schema.paths = {"s3://test-bucket/file.parquet"};
  schema.options["OSS_ENDPOINT"] = "https://oss-cn-hangzhou.aliyuncs.com";
  schema.options["OSS_REGION"] = "oss-cn-hangzhou";
  schema.options["CREDENTIALS_KIND"] = "explicit";
  schema.options["OSS_ACCESS_KEY_ID"] = "AKID123";
  schema.options["OSS_ACCESS_KEY_SECRET"] = "secret456";

  auto config = buildS3ClientConfig(schema);
  EXPECT_EQ(config.region, "oss-cn-hangzhou");
  EXPECT_EQ(config.access_key, "AKID123");
  EXPECT_EQ(config.secret_key, "secret456");
  EXPECT_EQ(config.credentials_kind,
            neug::extension::s3::NativeS3CredentialsKind::Explicit);
}

TEST(S3ClientConfigTest, BuildAnonymousCredentials) {
  FileSchema schema;
  schema.paths = {"s3://public-bucket/data.csv"};
  schema.options["CREDENTIALS_KIND"] = "anonymous";

  auto config = buildS3ClientConfig(schema);
  EXPECT_EQ(config.credentials_kind,
            neug::extension::s3::NativeS3CredentialsKind::Anonymous);
  EXPECT_TRUE(config.access_key.empty());
  EXPECT_TRUE(config.secret_key.empty());
}

TEST(S3ClientConfigTest, DefaultCredentials) {
  FileSchema schema;
  schema.paths = {"s3://bucket/key"};
  // No CREDENTIALS_KIND specified -> default

  auto config = buildS3ClientConfig(schema);
  EXPECT_EQ(config.credentials_kind,
            neug::extension::s3::NativeS3CredentialsKind::Default);
}

TEST(S3ClientConfigTest, EndpointOverrideFromOptions) {
  FileSchema schema;
  schema.paths = {"s3://bucket/key"};
  schema.options["AWS_ENDPOINT_URL"] = "http://localhost:9000";

  auto config = buildS3ClientConfig(schema);
  // The endpoint should be set (exact format depends on implementation)
  EXPECT_FALSE(config.endpoint_override.empty());
}

// ============================================================================
// HTTPFileSystem Native Mode Tests
// ============================================================================

TEST(HTTPFileSystemNativeTest, GlobReturnsPathUnchanged) {
  neug::common::case_insensitive_map_t<std::string> opts;
  HTTPFileSystem fs(opts);
  auto result = fs.glob("https://example.com/data.parquet");
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], "https://example.com/data.parquet");
}

TEST(HTTPFileSystemNativeTest, GetArrowFileSystemReturnsNull) {
  neug::common::case_insensitive_map_t<std::string> opts;
  HTTPFileSystem fs(opts);
  EXPECT_EQ(fs.getArrowFileSystem(), nullptr);
}

TEST(HTTPFileSystemNativeTest, CreateFromSchema) {
  FileSchema schema;
  schema.paths = {"https://example.com/file.parquet"};
  auto fs = CreateHTTPFileSystem(schema);
  EXPECT_NE(fs, nullptr);
  EXPECT_EQ(fs->getArrowFileSystem(), nullptr);
}

TEST(HTTPFileSystemNativeTest, InvalidURLInSchemaThrows) {
  FileSchema schema;
  schema.paths = {"ftp://invalid-scheme/file"};
  EXPECT_THROW(CreateHTTPFileSystem(schema), neug::exception::Exception);
}

// ============================================================================
// S3FileSystem Native Mode Tests
// ============================================================================

TEST(S3FileSystemNativeTest, GetArrowFileSystemReturnsNull) {
  FileSchema schema;
  schema.paths = {"s3://test-bucket/file.parquet"};
  schema.options["CREDENTIALS_KIND"] = "anonymous";
  schema.options["AWS_ENDPOINT_URL"] = "http://localhost:9000";

  S3FileSystem fs(schema);
  EXPECT_EQ(fs.getArrowFileSystem(), nullptr);
}

TEST(S3FileSystemNativeTest, GlobNonGlobReturnsNormalized) {
  FileSchema schema;
  schema.paths = {"s3://test-bucket/data/file.parquet"};
  schema.options["CREDENTIALS_KIND"] = "anonymous";
  schema.options["AWS_ENDPOINT_URL"] = "http://localhost:9000";

  S3FileSystem fs(schema);
  auto result = fs.glob("s3://test-bucket/data/file.parquet");
  // Non-glob path returns normalized as "bucket/key"
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], "test-bucket/data/file.parquet");
}

TEST(S3FileSystemNativeTest, EmptyPathsThrows) {
  FileSchema schema;
  schema.paths = {};
  schema.options["CREDENTIALS_KIND"] = "anonymous";

  EXPECT_THROW(S3FileSystem fs(schema), neug::exception::Exception);
}

TEST(S3FileSystemNativeTest, CreateFactory) {
  FileSchema schema;
  schema.paths = {"s3://bucket/key"};
  schema.options["CREDENTIALS_KIND"] = "anonymous";
  schema.options["AWS_ENDPOINT_URL"] = "http://localhost:9000";

  auto fs = CreateS3FileSystem(schema);
  EXPECT_NE(fs, nullptr);
  EXPECT_EQ(fs->getArrowFileSystem(), nullptr);
}
