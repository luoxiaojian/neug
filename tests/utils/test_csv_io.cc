/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/reader_utils.h"
#include "neug/utils/io/read/csv/csv_reader.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/vfs/file_system.h"
#include "neug/utils/io/write/writer.h"
#include "neug/utils/io/stream/output_stream.h"

namespace neug {
namespace test {

static constexpr const char* CSV_IO_TEST_DIR = "/tmp/csv_io_test";

class CsvIOTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (std::filesystem::exists(CSV_IO_TEST_DIR)) {
      std::filesystem::remove_all(CSV_IO_TEST_DIR);
    }
    std::filesystem::create_directories(CSV_IO_TEST_DIR);
  }

  void TearDown() override {
    if (std::filesystem::exists(CSV_IO_TEST_DIR)) {
      std::filesystem::remove_all(CSV_IO_TEST_DIR);
    }
  }

  void createFile(const std::string& filename, const std::string& content) {
    std::ofstream file(std::string(CSV_IO_TEST_DIR) + "/" + filename);
    file << content;
    file.close();
  }

  std::string filePath(const std::string& filename) {
    return std::string(CSV_IO_TEST_DIR) + "/" + filename;
  }

  std::shared_ptr<::common::DataType> createInt32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    return type;
  }

  std::shared_ptr<::common::DataType> createInt64Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
    return type;
  }

  std::shared_ptr<::common::DataType> createDoubleType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    return type;
  }

  std::shared_ptr<::common::DataType> createStringType() {
    auto type = std::make_shared<::common::DataType>();
    auto strType = std::make_unique<::common::String>();
    auto varChar = std::make_unique<::common::String::VarChar>();
    strType->set_allocated_var_char(varChar.release());
    type->set_allocated_string(strType.release());
    return type;
  }

  std::shared_ptr<::common::DataType> createBoolType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    return type;
  }

  std::shared_ptr<reader::ReadSharedState> createSharedState(
      const std::string& csvFile, const std::vector<std::string>& columnNames,
      const std::vector<std::shared_ptr<::common::DataType>>& columnTypes,
      const common::case_insensitive_map_t<std::string>& options = {},
      const std::vector<std::string>& projectColumns = {}) {
    auto sharedState = std::make_shared<reader::ReadSharedState>();
    auto entrySchema = std::make_shared<reader::TableEntrySchema>();
    entrySchema->columnNames = columnNames;
    entrySchema->columnTypes = columnTypes;

    reader::FileSchema fileSchema;
    fileSchema.paths = {filePath(csvFile)};
    fileSchema.format = "csv";
    fileSchema.options = options;

    reader::ExternalSchema externalSchema;
    externalSchema.entry = entrySchema;
    externalSchema.file = fileSchema;

    sharedState->schema = std::move(externalSchema);
    sharedState->projectColumns = projectColumns;
    return sharedState;
  }

  std::shared_ptr<reader::CsvReader> createCsvReader(
      const std::shared_ptr<reader::ReadSharedState>& sharedState) {
    auto optionsBuilder =
        std::make_unique<reader::CsvOptionsBuilder>(sharedState);
    fsys::FileSystemRegistry vfs;
    return std::make_shared<reader::CsvReader>(
        sharedState, std::move(optionsBuilder),
        vfs.Provide(sharedState->schema.file));
  }

  execution::Context readToContext(
      const std::shared_ptr<reader::CsvReader>& reader,
      const std::shared_ptr<reader::ReadSharedState>& sharedState) {
    return reader::toContext(reader->read(), *sharedState);
  }
};

// =============================================================================
// CSV Reader - Delimiter Variants
// =============================================================================

TEST_F(CsvIOTest, Reader_PipeDelimiter) {
  createFile("pipe.csv", "1|Alice|95.5\n2|Bob|87.0\n");
  auto state = createSharedState(
      "pipe.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

TEST_F(CsvIOTest, Reader_CommaDelimiter) {
  createFile("comma.csv", "1,Alice,95.5\n2,Bob,87.0\n");
  auto state = createSharedState(
      "comma.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"delim", ","}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

TEST_F(CsvIOTest, Reader_TabDelimiter) {
  createFile("tab.csv", "1\tAlice\t95.5\n2\tBob\t87.0\n");
  auto state = createSharedState(
      "tab.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"delim", "\t"}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

TEST_F(CsvIOTest, Reader_SemicolonDelimiter) {
  createFile("semi.csv", "1;Alice;95.5\n2;Bob;87.0\n");
  auto state = createSharedState(
      "semi.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"delim", ";"}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
}

// =============================================================================
// CSV Reader - Quoting and Escaping
// =============================================================================

TEST_F(CsvIOTest, Reader_QuotedFieldWithDelimiter) {
  // Field containing delimiter inside quotes
  createFile("quoted.csv", "1,\"Alice, Jr.\",95.5\n2,\"Bob\",87.0\n");
  auto state = createSharedState(
      "quoted.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"delim", ","}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 2);
  auto col1 = ctx.chunk(0).columns()[1];
  EXPECT_EQ(col1->get_elem(0).GetValue<std::string>(), "Alice, Jr.");
}

TEST_F(CsvIOTest, Reader_EmbeddedQuotes) {
  // Double-quote escaping: "" -> "
  createFile("dblquote.csv", "1,\"She said \"\"hi\"\"\",95.5\n");
  auto state = createSharedState(
      "dblquote.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"delim", ","}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 1);
  auto col1 = ctx.chunk(0).columns()[1];
  EXPECT_EQ(col1->get_elem(0).GetValue<std::string>(), "She said \"hi\"");
}

TEST_F(CsvIOTest, Reader_UnicodeContent) {
  createFile("unicode.csv", "1|你好|95.5\n2|世界|87.0\n");
  auto state = createSharedState(
      "unicode.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 2);
  auto col1 = ctx.chunk(0).columns()[1];
  EXPECT_EQ(col1->get_elem(0).GetValue<std::string>(), "你好");
  EXPECT_EQ(col1->get_elem(1).GetValue<std::string>(), "世界");
}

// =============================================================================
// CSV Reader - Type Conversion
// =============================================================================

TEST_F(CsvIOTest, Reader_Int32Column) {
  createFile("int32.csv", "10\n20\n30\n");
  auto state = createSharedState(
      "int32.csv", {"value"}, {createInt32Type()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 3);
  auto col = ctx.chunk(0).columns()[0];
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kInt32);
  EXPECT_EQ(col->get_elem(0).GetValue<int32_t>(), 10);
  EXPECT_EQ(col->get_elem(2).GetValue<int32_t>(), 30);
}

TEST_F(CsvIOTest, Reader_Int64Column) {
  createFile("int64.csv", "100000000000\n200000000000\n");
  auto state = createSharedState(
      "int64.csv", {"value"}, {createInt64Type()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 2);
  auto col = ctx.chunk(0).columns()[0];
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kInt64);
  EXPECT_EQ(col->get_elem(0).GetValue<int64_t>(), 100000000000LL);
}

TEST_F(CsvIOTest, Reader_DoubleColumn) {
  createFile("double.csv", "3.14\n2.718\n1.618\n");
  auto state = createSharedState(
      "double.csv", {"value"}, {createDoubleType()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 3);
  auto col = ctx.chunk(0).columns()[0];
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kDouble);
  EXPECT_DOUBLE_EQ(col->get_elem(0).GetValue<double>(), 3.14);
}

TEST_F(CsvIOTest, Reader_BoolColumn) {
  createFile("bool.csv", "true\nfalse\ntrue\n");
  auto state = createSharedState(
      "bool.csv", {"flag"}, {createBoolType()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 3);
  auto col = ctx.chunk(0).columns()[0];
  EXPECT_EQ(col->get_elem(0).GetValue<bool>(), true);
  EXPECT_EQ(col->get_elem(1).GetValue<bool>(), false);
}

TEST_F(CsvIOTest, Reader_MixedTypeColumns) {
  createFile("mixed.csv", "1|Alice|true|3.14\n2|Bob|false|2.71\n");
  auto state = createSharedState(
      "mixed.csv", {"id", "name", "active", "score"},
      {createInt32Type(), createStringType(), createBoolType(), createDoubleType()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 4);
  EXPECT_EQ(ctx.row_num(), 2);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<int32_t>(), 1);
  EXPECT_EQ(ctx.chunk(0).columns()[1]->get_elem(0).GetValue<std::string>(), "Alice");
  EXPECT_EQ(ctx.chunk(0).columns()[2]->get_elem(0).GetValue<bool>(), true);
  EXPECT_DOUBLE_EQ(ctx.chunk(0).columns()[3]->get_elem(0).GetValue<double>(), 3.14);
}

// =============================================================================
// CSV Reader - Edge Cases
// =============================================================================

TEST_F(CsvIOTest, Reader_EmptyFile) {
  createFile("empty.csv", "");
  auto state = createSharedState(
      "empty.csv", {"id"}, {createInt32Type()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  // Empty file triggers column mismatch exception
  EXPECT_THROW(readToContext(reader, state), exception::IOException);
}

TEST_F(CsvIOTest, Reader_HeaderOnly) {
  createFile("header_only.csv", "id|name|score\n");
  auto state = createSharedState(
      "header_only.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"skip_rows", "1"}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  // Header-only file with skip_rows triggers column mismatch exception
  EXPECT_THROW(readToContext(reader, state), exception::IOException);
}

TEST_F(CsvIOTest, Reader_SingleRow) {
  createFile("single.csv", "42|hello|1.5\n");
  auto state = createSharedState(
      "single.csv", {"id", "name", "val"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 1);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<int32_t>(), 42);
}

TEST_F(CsvIOTest, Reader_LargeFile) {
  std::string content;
  for (int i = 0; i < 10000; ++i) {
    content += std::to_string(i) + "|name_" + std::to_string(i) + "|" +
               std::to_string(i * 1.5) + "\n";
  }
  createFile("large.csv", content);
  auto state = createSharedState(
      "large.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 10000);
}

// =============================================================================
// CSV Reader - Column Projection
// =============================================================================

TEST_F(CsvIOTest, Reader_ProjectSingleColumn) {
  createFile("proj.csv", "1|Alice|95.5\n2|Bob|87.0\n");
  auto state = createSharedState(
      "proj.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}}, {"score"});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 1);
  EXPECT_EQ(ctx.row_num(), 2);
}

TEST_F(CsvIOTest, Reader_ProjectReorderedColumns) {
  createFile("reorder.csv", "1|Alice|95.5\n2|Bob|87.0\n");
  auto state = createSharedState(
      "reorder.csv", {"id", "name", "score"},
      {createInt32Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}}, {"score", "id"});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.col_num(), 2);
  // First column in output should be score
  EXPECT_EQ(ctx.chunk(0).columns()[0]->elem_type().id(), DataTypeId::kDouble);
}

// =============================================================================
// CSV Reader - Schema Inference
// =============================================================================

TEST_F(CsvIOTest, Reader_InferSchemaIntColumn) {
  createFile("infer_int.csv", "id|name\n1|a\n2|b\n3|c\n");
  auto state = createSharedState(
      "infer_int.csv", {"id", "name"},
      {createStringType(), createStringType()},
      {{"skip_rows", "1"}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto result = reader->inferSchema();
  ASSERT_TRUE(result.has_value());
  auto schema = result.value();
  // First column (integers) should be inferred as Int64
  EXPECT_EQ(schema->columnTypes[0]->primitive_type(),
            ::common::PrimitiveType::DT_SIGNED_INT64);
}

TEST_F(CsvIOTest, Reader_InferSchemaDoubleColumn) {
  createFile("infer_dbl.csv", "val\n1.5\n2.7\n3.14\n");
  auto state = createSharedState(
      "infer_dbl.csv", {"val"}, {createStringType()},
      {{"skip_rows", "1"}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto result = reader->inferSchema();
  ASSERT_TRUE(result.has_value());
  auto schema = result.value();
  EXPECT_EQ(schema->columnTypes[0]->primitive_type(),
            ::common::PrimitiveType::DT_DOUBLE);
}

TEST_F(CsvIOTest, Reader_InferSchemaStringColumn) {
  createFile("infer_str.csv", "name\nAlice\nBob\nCharlie\n");
  auto state = createSharedState(
      "infer_str.csv", {"name"}, {createStringType()},
      {{"skip_rows", "1"}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto result = reader->inferSchema();
  ASSERT_TRUE(result.has_value());
  auto schema = result.value();
  // String column should have varchar type
  EXPECT_TRUE(schema->columnTypes[0]->has_string());
}

// =============================================================================
// CSV Reader - Multi-file
// =============================================================================

TEST_F(CsvIOTest, Reader_MultipleFiles) {
  createFile("multi1.csv", "1|Alice\n2|Bob\n");
  createFile("multi2.csv", "3|Charlie\n4|David\n");
  auto state = createSharedState(
      "multi1.csv", {"id", "name"},
      {createInt32Type(), createStringType()},
      {{"batch_read", "false"}});
  state->schema.file.paths.push_back(filePath("multi2.csv"));
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 4);
}

// =============================================================================
// CSV Reader - Batch Mode
// =============================================================================

TEST_F(CsvIOTest, Reader_BatchMode) {
  std::string content;
  for (int i = 0; i < 100; ++i) {
    content += std::to_string(i) + "|name_" + std::to_string(i) + "\n";
  }
  createFile("batch.csv", content);
  auto state = createSharedState(
      "batch.csv", {"id", "name"},
      {createInt32Type(), createStringType()},
      {{"batch_read", "true"}, {"batch_size", "10"}});
  auto reader = createCsvReader(state);
  auto supplier = reader->read();
  ASSERT_NE(supplier, nullptr);
  // Should be able to iterate through chunks
  int total_rows = 0;
  while (auto chunk = supplier->GetNextChunk()) {
    total_rows += static_cast<int>(chunk->row_num());
  }
  EXPECT_EQ(total_rows, 100);
}

// =============================================================================
// CSV Writer - CSVStringFormatBuffer
// =============================================================================

TEST_F(CsvIOTest, Writer_Int32Array) {
  neug::QueryResponse response;
  response.set_row_count(3);
  auto* col = response.add_arrays();
  auto* int_arr = col->mutable_int32_array();
  int_arr->add_values(10);
  int_arr->add_values(20);
  int_arr->add_values(30);
  int_arr->set_validity(std::string(1, 0xFF));

  reader::FileSchema schema;
  schema.options = {{"delim", ","}, {"header", "false"}};
  reader::TableEntrySchema entry;
  entry.columnNames = {"value"};

  writer::CSVStringFormatBuffer buf(&response, schema, entry);
  buf.addValue(0, 0);
  buf.addValue(1, 0);
  buf.addValue(2, 0);

  auto stream = io::openLocalOutputStream(filePath("out_int32.csv"));
  ASSERT_NE(stream, nullptr);
  EXPECT_TRUE(buf.flush(*stream).ok());
  EXPECT_TRUE(stream->Close().ok());

  // Read back and verify
  std::ifstream in(filePath("out_int32.csv"));
  std::string content((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());
  EXPECT_NE(content.find("10"), std::string::npos);
  EXPECT_NE(content.find("20"), std::string::npos);
  EXPECT_NE(content.find("30"), std::string::npos);
}

TEST_F(CsvIOTest, Writer_StringArrayWithEscaping) {
  neug::QueryResponse response;
  response.set_row_count(2);
  auto* col = response.add_arrays();
  auto* str_arr = col->mutable_string_array();
  str_arr->add_values("hello, world");  // contains comma
  str_arr->add_values("say \"hi\"");    // contains quotes
  str_arr->set_validity(std::string(1, 0xFF));

  reader::FileSchema schema;
  schema.options = {{"delim", ","}, {"header", "false"},
                    {"quote", "\""}, {"escape", "\\"}};
  reader::TableEntrySchema entry;
  entry.columnNames = {"msg"};

  writer::CSVStringFormatBuffer buf(&response, schema, entry);
  buf.addValue(0, 0);
  buf.addValue(1, 0);

  auto stream = io::openLocalOutputStream(filePath("out_str.csv"));
  ASSERT_NE(stream, nullptr);
  EXPECT_TRUE(buf.flush(*stream).ok());
  EXPECT_TRUE(stream->Close().ok());

  std::ifstream in(filePath("out_str.csv"));
  std::string content((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());
  // The string containing comma should be quoted
  EXPECT_NE(content.find("\"hello, world\""), std::string::npos);
}

TEST_F(CsvIOTest, Writer_DoubleArray) {
  neug::QueryResponse response;
  response.set_row_count(2);
  auto* col = response.add_arrays();
  auto* dbl_arr = col->mutable_double_array();
  dbl_arr->add_values(3.14);
  dbl_arr->add_values(2.718);
  dbl_arr->set_validity(std::string(1, 0xFF));

  reader::FileSchema schema;
  schema.options = {{"delim", ","}, {"header", "false"}};
  reader::TableEntrySchema entry;
  entry.columnNames = {"val"};

  writer::CSVStringFormatBuffer buf(&response, schema, entry);
  buf.addValue(0, 0);
  buf.addValue(1, 0);

  auto stream = io::openLocalOutputStream(filePath("out_dbl.csv"));
  ASSERT_NE(stream, nullptr);
  EXPECT_TRUE(buf.flush(*stream).ok());
  EXPECT_TRUE(stream->Close().ok());

  std::ifstream in(filePath("out_dbl.csv"));
  std::string content((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());
  EXPECT_NE(content.find("3.14"), std::string::npos);
}

TEST_F(CsvIOTest, Writer_BoolArray) {
  neug::QueryResponse response;
  response.set_row_count(2);
  auto* col = response.add_arrays();
  auto* bool_arr = col->mutable_bool_array();
  bool_arr->add_values(true);
  bool_arr->add_values(false);
  bool_arr->set_validity(std::string(1, 0xFF));

  reader::FileSchema schema;
  schema.options = {{"delim", ","}, {"header", "false"}};
  reader::TableEntrySchema entry;
  entry.columnNames = {"flag"};

  writer::CSVStringFormatBuffer buf(&response, schema, entry);
  buf.addValue(0, 0);
  buf.addValue(1, 0);

  auto stream = io::openLocalOutputStream(filePath("out_bool.csv"));
  ASSERT_NE(stream, nullptr);
  EXPECT_TRUE(buf.flush(*stream).ok());
  EXPECT_TRUE(stream->Close().ok());

  std::ifstream in(filePath("out_bool.csv"));
  std::string content((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());
  EXPECT_NE(content.find("1"), std::string::npos);
  EXPECT_NE(content.find("0"), std::string::npos);
}

TEST_F(CsvIOTest, Writer_WithHeader) {
  neug::QueryResponse response;
  response.set_row_count(1);
  auto* col = response.add_arrays();
  auto* int_arr = col->mutable_int32_array();
  int_arr->add_values(42);
  int_arr->set_validity(std::string(1, 0xFF));

  reader::FileSchema schema;
  schema.options = {{"delim", ","}, {"header", "true"}};
  reader::TableEntrySchema entry;
  entry.columnNames = {"answer"};

  writer::CSVStringFormatBuffer buf(&response, schema, entry);
  buf.addHeader();
  buf.addValue(0, 0);

  auto stream = io::openLocalOutputStream(filePath("out_header.csv"));
  ASSERT_NE(stream, nullptr);
  EXPECT_TRUE(buf.flush(*stream).ok());
  EXPECT_TRUE(stream->Close().ok());

  std::ifstream in(filePath("out_header.csv"));
  std::string line;
  std::getline(in, line);
  EXPECT_EQ(line, "answer");
}

TEST_F(CsvIOTest, Writer_MultiColumn) {
  neug::QueryResponse response;
  response.set_row_count(2);

  auto* col0 = response.add_arrays();
  auto* int_arr = col0->mutable_int32_array();
  int_arr->add_values(1);
  int_arr->add_values(2);
  int_arr->set_validity(std::string(1, 0xFF));

  auto* col1 = response.add_arrays();
  auto* str_arr = col1->mutable_string_array();
  str_arr->add_values("Alice");
  str_arr->add_values("Bob");
  str_arr->set_validity(std::string(1, 0xFF));

  reader::FileSchema schema;
  schema.options = {{"delim", "|"}, {"header", "true"}};
  reader::TableEntrySchema entry;
  entry.columnNames = {"id", "name"};

  writer::CSVStringFormatBuffer buf(&response, schema, entry);
  buf.addHeader();
  for (int row = 0; row < 2; ++row) {
    for (int col = 0; col < 2; ++col) {
      buf.addValue(row, col);
    }
  }

  auto stream = io::openLocalOutputStream(filePath("out_multi.csv"));
  ASSERT_NE(stream, nullptr);
  EXPECT_TRUE(buf.flush(*stream).ok());
  EXPECT_TRUE(stream->Close().ok());

  std::ifstream in(filePath("out_multi.csv"));
  std::string line;
  std::getline(in, line);
  EXPECT_EQ(line, "id|name");
  std::getline(in, line);
  EXPECT_EQ(line, "1|\"Alice\"");
}

// =============================================================================
// CSV Writer - CsvQueryExportWriter round-trip
// =============================================================================

TEST_F(CsvIOTest, Writer_ExportAndReadBack) {
  neug::QueryResponse response;
  response.set_row_count(3);

  auto* col0 = response.add_arrays();
  auto* int_arr = col0->mutable_int64_array();
  int_arr->add_values(100);
  int_arr->add_values(200);
  int_arr->add_values(300);
  int_arr->set_validity(std::string(1, 0xFF));

  auto* col1 = response.add_arrays();
  auto* dbl_arr = col1->mutable_double_array();
  dbl_arr->add_values(1.1);
  dbl_arr->add_values(2.2);
  dbl_arr->add_values(3.3);
  dbl_arr->set_validity(std::string(1, 0xFF));

  reader::FileSchema schema;
  schema.paths = {filePath("roundtrip.csv")};
  schema.options = {{"delim", "|"}, {"header", "true"}, {"batch_size", "1024"}};
  auto entry = std::make_shared<reader::TableEntrySchema>();
  entry->columnNames = {"id", "score"};

  writer::CsvQueryExportWriter writer(schema, entry);
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Read back
  auto state = createSharedState(
      "roundtrip.csv", {"id", "score"},
      {createInt64Type(), createDoubleType()},
      {{"skip_rows", "1"}, {"batch_read", "false"}});
  auto reader = createCsvReader(state);
  auto ctx = readToContext(reader, state);
  EXPECT_EQ(ctx.row_num(), 3);
  EXPECT_EQ(ctx.chunk(0).columns()[0]->get_elem(0).GetValue<int64_t>(), 100);
  EXPECT_DOUBLE_EQ(ctx.chunk(0).columns()[1]->get_elem(0).GetValue<double>(), 1.1);
}

}  // namespace test
}  // namespace neug
