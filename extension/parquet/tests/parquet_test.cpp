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
#include <filesystem>
#include <memory>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/reader_utils.h"
#include "neug/utils/io/reader.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/vfs/file_system.h"

#include "../../extension/parquet/include/parquet/parquet_reader.h"
#include "parquet_test_helpers.h"
#include "../../extension/parquet/include/parquet_options.h"
#include "../../extension/parquet/include/parquet/parquet_export_writer.h"
#include "neug/generated/proto/response/response.pb.h"
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#endif

namespace neug {
namespace test {

static constexpr const char* PARQUET_TEST_DIR = "/tmp/parquet_test";

class ParquetTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (std::filesystem::exists(PARQUET_TEST_DIR)) {
      std::filesystem::remove_all(PARQUET_TEST_DIR);
    }
    std::filesystem::create_directories(PARQUET_TEST_DIR);
  }

  void TearDown() override {
    if (std::filesystem::exists(PARQUET_TEST_DIR)) {
      std::filesystem::remove_all(PARQUET_TEST_DIR);
    }
  }

  // Helper function to create a simple Parquet file
  void createSimpleParquetFile(const std::string& filename) {
    writeSimpleParquetFile(PARQUET_TEST_DIR, filename);
  }


  // Helper function to create DataType as shared_ptr
  std::shared_ptr<::common::DataType> createInt64Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT64);
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

  std::shared_ptr<::common::DataType> createDoubleType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_DOUBLE);
    return type;
  }

  std::shared_ptr<::common::DataType> createInt32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_SIGNED_INT32);
    return type;
  }

  std::shared_ptr<::common::DataType> createBoolType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_BOOL);
    return type;
  }

  std::shared_ptr<::common::DataType> createDateType() {
    auto type = std::make_shared<::common::DataType>();
    auto temporal = std::make_unique<::common::Temporal>();
    temporal->mutable_date();  // Date type
    type->set_allocated_temporal(temporal.release());
    return type;
  }

  std::shared_ptr<::common::DataType> createTimestampType() {
    auto type = std::make_shared<::common::DataType>();
    auto temporal = std::make_unique<::common::Temporal>();
    temporal->mutable_timestamp();  // Timestamp type
    type->set_allocated_temporal(temporal.release());
    return type;
  }

  std::shared_ptr<::common::DataType> createFloatType() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_FLOAT);
    return type;
  }

  std::shared_ptr<::common::DataType> createUint32Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT32);
    return type;
  }

  std::shared_ptr<::common::DataType> createUint64Type() {
    auto type = std::make_shared<::common::DataType>();
    type->set_primitive_type(::common::PrimitiveType::DT_UNSIGNED_INT64);
    return type;
  }

  std::shared_ptr<::common::DataType> createIntervalType() {
    auto type = std::make_shared<::common::DataType>();
    auto temporal = std::make_unique<::common::Temporal>();
    temporal->mutable_interval();
    type->set_allocated_temporal(temporal.release());
    return type;
  }

  std::shared_ptr<::common::DataType> createMapStringStringType() {
    auto type = std::make_shared<::common::DataType>();
    auto* map = type->mutable_map();
    *map->mutable_key_type() = *createStringType();
    *map->mutable_value_type() = *createStringType();
    return type;
  }

  std::shared_ptr<::common::DataType> createListStringType() {
    auto type = std::make_shared<::common::DataType>();
    auto* array = type->mutable_array();
    auto* component = array->mutable_component_type();
    auto strType = std::make_unique<::common::String>();
    auto varChar = std::make_unique<::common::String::VarChar>();
    strType->set_allocated_var_char(varChar.release());
    component->set_allocated_string(strType.release());
    return type;
  }

  std::shared_ptr<::common::DataType> createLocationTupleType() {
    auto type = std::make_shared<::common::DataType>();
    auto* tuple = type->mutable_tuple();
    *tuple->add_component_types() = *createDoubleType();
    *tuple->add_component_types() = *createDoubleType();
    return type;
  }

  void appendMapListColumn(Array* col, int num_entries,
                           const std::vector<std::string>& keys,
                           const std::vector<std::string>& values) {
    auto* list_arr = col->mutable_list_array();
    list_arr->add_offsets(0);
    list_arr->add_offsets(num_entries);
    list_arr->set_validity(std::string(1, 0xFF));
    auto* struct_arr = list_arr->mutable_elements()->mutable_struct_array();
    auto* key_field = struct_arr->add_fields()->mutable_string_array();
    auto* val_field = struct_arr->add_fields()->mutable_string_array();
    for (int i = 0; i < num_entries; ++i) {
      key_field->add_values(keys[static_cast<size_t>(i)]);
      val_field->add_values(values[static_cast<size_t>(i)]);
    }
    key_field->set_validity(std::string(1, 0xFF));
    val_field->set_validity(std::string(1, 0xFF));
  }

  // Helper function to create ReadSharedState
  std::shared_ptr<reader::ReadSharedState> createSharedState(
      const std::string& parquetFile,
      const std::vector<std::string>& columnNames,
      const std::vector<std::shared_ptr<::common::DataType>>& columnTypes,
      const common::case_insensitive_map_t<std::string>& options = {}) {
    auto sharedState = std::make_shared<reader::ReadSharedState>();

    auto entrySchema = std::make_shared<reader::TableEntrySchema>();
    entrySchema->columnNames = columnNames;
    entrySchema->columnTypes = columnTypes;

    // Create FileSchema
    reader::FileSchema fileSchema;
    fileSchema.paths = {std::string(PARQUET_TEST_DIR) + "/" + parquetFile};
    fileSchema.format = "parquet";
    fileSchema.options = options;

    // Create ExternalSchema
    reader::ExternalSchema externalSchema;
    externalSchema.entry = entrySchema;
    externalSchema.file = fileSchema;

    sharedState->schema = std::move(externalSchema);

    return sharedState;
  }

  execution::Context readToContext(
      const std::shared_ptr<reader::ParquetReader>& reader,
      const std::shared_ptr<reader::ReadSharedState>& sharedState) {
    return reader::toContext(reader->read(), *sharedState);
  }

  std::shared_ptr<reader::ParquetReader> createParquetReader(
      const std::shared_ptr<reader::ReadSharedState>& sharedState) {
    fsys::FileSystemRegistry registry;
    auto fs = registry.Provide(sharedState->schema.file);
    auto optionsBuilder =
        std::make_unique<reader::ParquetOptionsBuilder>(sharedState);
    return std::make_shared<reader::ParquetReader>(
        sharedState, std::move(optionsBuilder), std::move(fs));
  }

  neug::writer::ParquetExportWriter createExportWriter(
      const reader::FileSchema& file_schema,
      const std::shared_ptr<reader::EntrySchema>& entry_schema = nullptr) {
    fsys::FileSystemRegistry registry;
    auto fs = registry.Provide(file_schema);
    return neug::writer::ParquetExportWriter(
        file_schema, std::move(fs), entry_schema);
  }
};

#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
// =============================================================================
// Test Suite 1: Options Translation Tests
// Verify that Neug options are correctly translated to Arrow Parquet configuration
// =============================================================================

TEST_F(ParquetTest, TestOptionsBuilder_BuildsValidParquetReadOptions) {
  createSimpleParquetFile("test_options.parquet");
  
  auto sharedState = createSharedState(
      "test_options.parquet", 
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {});
  
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  EXPECT_NE(options.reader_properties, nullptr)
      << "Extension should initialize reader_properties";
  EXPECT_NE(options.arrow_reader_properties, nullptr)
      << "Extension should initialize arrow_reader_properties";
}

TEST_F(ParquetTest, TestOptionsTranslation_BufferSize) {
  createSimpleParquetFile("test_buffer.parquet");
  
  // Test custom buffer_size option
  const int64_t custom_buffer_size = 2048;
  auto sharedState = createSharedState(
      "test_buffer.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_size", std::to_string(custom_buffer_size)}});
  
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  ASSERT_NE(options.reader_properties, nullptr);
  
  EXPECT_EQ(options.reader_properties->buffer_size(), custom_buffer_size)
      << "Extension should translate batch_size option to Arrow buffer_size";
}

TEST_F(ParquetTest, TestOptionsTranslation_ParquetBatchRows) {
  createSimpleParquetFile("test_batch_rows.parquet");
  
  // Test PARQUET_BATCH_ROWS option
  const int64_t custom_batch_rows = 4096;
  auto sharedState = createSharedState(
      "test_batch_rows.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"PARQUET_BATCH_ROWS", std::to_string(custom_batch_rows)}});
  
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  ASSERT_NE(options.arrow_reader_properties, nullptr);
  
  EXPECT_EQ(options.arrow_reader_properties->batch_size(), custom_batch_rows)
      << "Extension should translate PARQUET_BATCH_ROWS to Arrow batch_size";
}

TEST_F(ParquetTest, TestOptionsTranslation_PreBuffer) {
  createSimpleParquetFile("test_prebuffer.parquet");
  
  // Test PRE_BUFFER=true (default is false)
  auto sharedState = createSharedState(
      "test_prebuffer.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"PRE_BUFFER", "true"}});
  
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  ASSERT_NE(options.arrow_reader_properties, nullptr);
  
  EXPECT_TRUE(options.arrow_reader_properties->pre_buffer())
      << "Extension should translate PRE_BUFFER=true to Arrow pre_buffer setting";
}

TEST_F(ParquetTest, TestOptionsTranslation_UseThreads) {
  createSimpleParquetFile("test_threads.parquet");
  
  // Test parallel=false (use_threads)
  auto sharedState = createSharedState(
      "test_threads.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"parallel", "false"}});
  
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  ASSERT_NE(options.arrow_reader_properties, nullptr);
  
  EXPECT_FALSE(options.arrow_reader_properties->use_threads())
      << "Extension should translate parallel=false to use_threads=false";
}

TEST_F(ParquetTest, TestOptionsTranslation_IoCoalescing) {
  createSimpleParquetFile("test_cache.parquet");
  
  // Test ENABLE_IO_COALESCING=true (default) — should use LazyDefaults (lazy=true)
  auto sharedState1 = createSharedState(
      "test_cache.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"ENABLE_IO_COALESCING", "true"}});
  
  reader::ParquetOptionsBuilder optionsBuilder1(sharedState1);
  auto options1 = optionsBuilder1.build();
  
  ASSERT_NE(options1.arrow_reader_properties, nullptr);
  
  auto cache_opts1 = options1.arrow_reader_properties->cache_options();
  EXPECT_TRUE(cache_opts1.lazy)
      << "Extension should use LazyDefaults (lazy=true) when ENABLE_IO_COALESCING=true";
  
  // Test ENABLE_IO_COALESCING=false — should use Defaults (lazy=false, eager coalescing)
  auto sharedState2 = createSharedState(
      "test_cache.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"ENABLE_IO_COALESCING", "false"}});
  
  reader::ParquetOptionsBuilder optionsBuilder2(sharedState2);
  auto options2 = optionsBuilder2.build();
  
  auto cache_opts2 = options2.arrow_reader_properties->cache_options();
  EXPECT_FALSE(cache_opts2.lazy)
      << "Extension should use Defaults (lazy=false) when ENABLE_IO_COALESCING=false";
}

TEST_F(ParquetTest, TestOptionsTranslation_DefaultValues) {
  createSimpleParquetFile("test_defaults.parquet");
  
  // Create state without any options - should use defaults
  auto sharedState = createSharedState(
      "test_defaults.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {});
  
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  ASSERT_NE(options.arrow_reader_properties, nullptr);
  
  EXPECT_EQ(options.arrow_reader_properties->batch_size(), 65536)
      << "Extension should use default PARQUET_BATCH_ROWS=65536";
  
  EXPECT_FALSE(options.arrow_reader_properties->pre_buffer())
      << "Extension should use default PRE_BUFFER=false";
  
  EXPECT_TRUE(options.arrow_reader_properties->use_threads())
      << "Extension should use default parallel=true";
}

TEST_F(ParquetTest, TestFileFormatConfiguration_UsesParquetBatchRows) {
  createSimpleParquetFile("test_format.parquet");
  
  auto sharedState = createSharedState(
      "test_format.parquet", 
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"PARQUET_BATCH_ROWS", "2048"}});
  
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  
  ASSERT_NE(options.arrow_reader_properties, nullptr);
  EXPECT_EQ(options.arrow_reader_properties->batch_size(), 2048);
}


#else

TEST_F(ParquetTest, TestCarquetOptions_BuildsValidParquetReadOptions) {
  createSimpleParquetFile("test_options_carquet.parquet");
  auto sharedState = createSharedState(
      "test_options_carquet.parquet", {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()}, {});
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  EXPECT_EQ(options.batch_size, 65536);
  EXPECT_TRUE(options.use_threads);
}

TEST_F(ParquetTest, TestCarquetOptions_ParquetBatchRows) {
  createSimpleParquetFile("test_batch_rows_carquet.parquet");
  const int64_t custom_batch_rows = 4096;
  auto sharedState = createSharedState(
      "test_batch_rows_carquet.parquet", {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"PARQUET_BATCH_ROWS", std::to_string(custom_batch_rows)}});
  reader::ParquetOptionsBuilder optionsBuilder(sharedState);
  auto options = optionsBuilder.build();
  EXPECT_EQ(options.batch_size, custom_batch_rows);
}

#endif

// =============================================================================
// Test Suite 2: Type Mapping Tests
// Verify type conversion between Neug DataType and Arrow types
// =============================================================================

TEST_F(ParquetTest, TestTypeMapping_StringToLargeUtf8) {
  createSimpleParquetFile("test_string_type.parquet");
  
  // Neug uses STRING type, Arrow Parquet may have utf8
  // Extension should convert to large_utf8 for consistency
  auto sharedState = createSharedState(
      "test_string_type.parquet", 
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);

  // Verify string column is converted to large_utf8
  auto col1 = ctx.chunk(0).columns()[1];
  ASSERT_EQ(col1->column_type(), execution::ContextColumnType::kValue);
  EXPECT_EQ(col1->elem_type().id(), neug::DataTypeId::kVarchar);
}

TEST_F(ParquetTest, TestTypeMapping_PreserveNumericTypes) {
  writeNumericTypesParquetFile(PARQUET_TEST_DIR, "test_numeric_types.parquet");

  // Read with NeuG types
  auto sharedState = createSharedState(
      "test_numeric_types.parquet",
      {"int32_col", "int64_col", "double_col", "bool_col"},
      {createInt32Type(), createInt64Type(), createDoubleType(), createBoolType()},
      {{"batch_read", "false"}});

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);

  EXPECT_EQ(ctx.col_num(), 4);
  EXPECT_EQ(ctx.row_num(), 1);

  EXPECT_EQ(ctx.chunk(0).columns()[0]->elem_type().id(), neug::DataTypeId::kInt32);
  EXPECT_EQ(ctx.chunk(0).columns()[1]->elem_type().id(), neug::DataTypeId::kInt64);
  EXPECT_EQ(ctx.chunk(0).columns()[2]->elem_type().id(), neug::DataTypeId::kDouble);
  EXPECT_EQ(ctx.chunk(0).columns()[3]->elem_type().id(), neug::DataTypeId::kBoolean);
}

// =============================================================================
// Test Suite 3: Integration with Neug Query System
// Verify filter pushdown and column pruning work through the extension
// =============================================================================

TEST_F(ParquetTest, TestIntegration_ColumnPruning) {
  writePruningParquetFile(PARQUET_TEST_DIR, "test_pruning.parquet");
  const std::string filepath = std::string(PARQUET_TEST_DIR) + "/test_pruning.parquet";

  // Set up shared state with projectColumns
  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"id", "name", "score", "grade"};
  entrySchema->columnTypes = {createInt32Type(), createStringType(), 
                               createDoubleType(), createStringType()};

  reader::FileSchema fileSchema;
  fileSchema.paths = {filepath};
  fileSchema.format = "parquet";
  fileSchema.options = {{"batch_read", "false"}};

  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);
  
  // Neug's column projection: id, score, grade (exclude "name")
  sharedState->projectColumns = {"id", "score", "grade"};

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);

  // Verify extension translates projectColumns to Arrow projection
  // Should have 3 columns (id, score, grade - "name" is excluded)
  EXPECT_EQ(ctx.col_num(), 3)
      << "Extension should translate Neug's projectColumns to Arrow column projection";
  EXPECT_EQ(sharedState->columnNum(), 3)
      << "Extension should update columnNum after projection";
}

TEST_F(ParquetTest, TestIntegration_FilterPushdown) {
  writeFilterParquetFile(PARQUET_TEST_DIR, "test_filter.parquet");
  const std::string filepath = std::string(PARQUET_TEST_DIR) + "/test_filter.parquet";

  // Create Neug filter expression: score > 90.0
  auto filterExpr = std::make_shared<::common::Expression>();
  
  auto var_opr = filterExpr->add_operators();
  auto var = var_opr->mutable_var();
  var->mutable_tag()->set_name("score");
  
  auto gt_opr = filterExpr->add_operators();
  gt_opr->set_logical(::common::Logical::GT);
  
  auto const_opr = filterExpr->add_operators();
  const_opr->mutable_const_()->set_f64(90.0);

  // Set up shared state with filter
  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"id", "score"};
  entrySchema->columnTypes = {createInt32Type(), createDoubleType()};

  reader::FileSchema fileSchema;
  fileSchema.paths = {filepath};
  fileSchema.format = "parquet";
  fileSchema.options = {{"batch_read", "false"}};

  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);
  sharedState->skipRows = filterExpr;  // Neug's filter expression

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);

  // Verify extension translates Neug filter to Arrow filter
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(ctx.row_num(), 3)
      << "Extension should translate Neug's skipRows filter to Arrow filter pushdown. "
      << "Should filter to 3 rows with score > 90.0";
  
  // Verify the filtered data (ValueColumn after decode)
  auto col1 = std::dynamic_pointer_cast<execution::ValueColumn<double>>(ctx.chunk(0).columns()[1]);
  ASSERT_NE(col1, nullptr);
  for (size_t i = 0; i < col1->size(); ++i) {
    EXPECT_GT(col1->get_value(i), 90.0)
        << "Extension filter should result in all scores > 90.0";
  }
}

TEST_F(ParquetTest, TestIntegration_BatchReadMode) {
  writeBatchModeParquetFile(PARQUET_TEST_DIR, "test_batch_mode.parquet");

  auto sharedState = createSharedState(
      "test_batch_mode.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "true"}});

  auto reader = createParquetReader(sharedState);
  auto supplier = reader->read();
  ASSERT_NE(supplier, nullptr);
  int total_rows = 0;
  while (auto chunk = supplier->GetNextChunk()) {
    EXPECT_EQ(chunk->col_num(), 3u);
    total_rows += static_cast<int>(chunk->row_num());
  }
  EXPECT_GT(total_rows, 0);

  auto sharedState2 = createSharedState(
      "test_batch_mode.parquet",
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});

  auto reader2 = createParquetReader(sharedState2);
  execution::Context ctx2 = readToContext(reader2, sharedState2);

  EXPECT_EQ(ctx2.col_num(), 3);
  auto col0_2 = ctx2.chunk(0).columns()[0];
  EXPECT_EQ(col0_2->column_type(), execution::ContextColumnType::kValue)
      << "Extension should use Value column type when batch_read=false";
}

TEST_F(ParquetTest, TestIntegration_CombinedFilterAndProjection) {
  writeCombinedParquetFile(PARQUET_TEST_DIR, "test_combined.parquet");
  const std::string filepath = std::string(PARQUET_TEST_DIR) + "/test_combined.parquet";

  auto filterExpr = std::make_shared<::common::Expression>();
  auto var_opr = filterExpr->add_operators();
  var_opr->mutable_var()->mutable_tag()->set_name("score");
  auto gt_opr = filterExpr->add_operators();
  gt_opr->set_logical(::common::Logical::GT);
  auto const_opr = filterExpr->add_operators();
  const_opr->mutable_const_()->set_f64(90.0);

  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"id", "name", "score", "grade"};
  entrySchema->columnTypes = {createInt32Type(), createStringType(),
                               createDoubleType(), createStringType()};

  reader::FileSchema fileSchema;
  fileSchema.paths = {filepath};
  fileSchema.format = "parquet";
  fileSchema.options = {{"batch_read", "false"}};

  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);
  sharedState->projectColumns = {"id", "score", "grade"};
  sharedState->skipRows = filterExpr;

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);

  EXPECT_EQ(ctx.col_num(), 3)
      << "Extension should apply column pruning (3 of 4 columns)";
  EXPECT_EQ(ctx.row_num(), 2)
      << "Extension should apply filter (2 rows with score > 90.0)";
  EXPECT_EQ(sharedState->columnNum(), 3)
      << "Extension should update columnNum after pruning";
}

// =============================================================================
// Test Suite 4: Multi-file Handling
// Verify extension correctly handles multiple Parquet files
// =============================================================================

TEST_F(ParquetTest, TestMultiFile_ExplicitPaths) {
  for (int fileIdx = 0; fileIdx < 3; ++fileIdx) {
    writeMultiFileParquet(PARQUET_TEST_DIR,
                          "test_multi_" + std::to_string(fileIdx) + ".parquet",
                          fileIdx * 10, 10);
  }

  // Extension should handle multiple explicit file paths
  auto sharedState = std::make_shared<reader::ReadSharedState>();
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"id"};
  entrySchema->columnTypes = {createInt32Type()};

  reader::FileSchema fileSchema;
  fileSchema.paths = {
      std::string(PARQUET_TEST_DIR) + "/test_multi_0.parquet",
      std::string(PARQUET_TEST_DIR) + "/test_multi_1.parquet",
      std::string(PARQUET_TEST_DIR) + "/test_multi_2.parquet"
  };
  fileSchema.format = "parquet";
  fileSchema.options = {{"batch_read", "false"}};

  reader::ExternalSchema externalSchema;
  externalSchema.entry = entrySchema;
  externalSchema.file = fileSchema;
  sharedState->schema = std::move(externalSchema);

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);

  EXPECT_EQ(ctx.col_num(), 1);
  EXPECT_EQ(ctx.row_num(), 30)
      << "Extension should correctly read and concatenate multiple Parquet files";
}

// =============================================================================
// Test Suite: Parquet Export Tests
// Test ParquetExportWriter functionality (Arrow + Carquet backends)
// =============================================================================

TEST_F(ParquetTest, TestParquetExportWriter) {
  // Create a QueryResponse with test data
  neug::QueryResponse response;
  response.set_row_count(3);
  
  // Add schema
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("name");
  schema->add_name("value");
  
  // Column 0: int64 array
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  int64_arr->add_values(1);
  int64_arr->add_values(2);
  int64_arr->add_values(3);
  
  // Column 1: string array
  auto* col1 = response.add_arrays();
  auto* str_arr = col1->mutable_string_array();
  str_arr->add_values("Alice");
  str_arr->add_values("Bob");
  str_arr->add_values("Charlie");
  
  // Column 2: double array
  auto* col2 = response.add_arrays();
  auto* double_arr = col2->mutable_double_array();
  double_arr->add_values(10.5);
  double_arr->add_values(20.3);
  double_arr->add_values(30.7);
  
  // Create EntrySchema with types
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "name", "value"};
  entry_schema->columnTypes = {createInt64Type(), createStringType(), createDoubleType()};
  
  // Create FileSchema
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_writer_test.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  // Create ParquetExportWriter
  auto writer = createExportWriter(file_schema, entry_schema);
  
  // Write the response
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet: " << status.ToString();
  
  // Verify file was created
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Read it back and verify
  auto sharedState = createSharedState(
      "export_writer_test.parquet", 
      {"id", "name", "value"},
      {createInt64Type(), createStringType(), createDoubleType()},
      {{"batch_read", "false"}});
  
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  
  EXPECT_EQ(ctx.col_num(), 3);
  EXPECT_EQ(ctx.row_num(), 3);
}

TEST_F(ParquetTest, TestParquetExportWithNulls) {
  // Test export with NULL values
  neug::QueryResponse response;
  response.set_row_count(3);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("name");
  
  // Column 0: int64 array with some nulls
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  int64_arr->add_values(1);
  int64_arr->add_values(2);
  int64_arr->add_values(3);
  // Validity bitmap: 1, 0, 1 (second value is null)
  int64_arr->set_validity("\x05");  // binary: 00000101
  
  // Column 1: string array with some nulls
  auto* col1 = response.add_arrays();
  auto* str_arr = col1->mutable_string_array();
  str_arr->add_values("Alice");
  str_arr->add_values("Bob");
  str_arr->add_values("Charlie");
  str_arr->set_validity("\x07");  // all valid: 00000111
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "name"};
  entry_schema->columnTypes = {createInt64Type(), createStringType()};
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_nulls_test.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with nulls: " << status.ToString();
  
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Read back and verify
  auto sharedState = createSharedState(
      "export_nulls_test.parquet", 
      {"id", "name"},
      {createInt64Type(), createStringType()},
      {{"batch_read", "false"}});
  
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(ctx.row_num(), 3);
}

TEST_F(ParquetTest, TestParquetExportMultipleTypes) {
  // Test export with various data types
  neug::QueryResponse response;
  response.set_row_count(2);
  
  auto* schema = response.mutable_schema();
  schema->add_name("int32_col");
  schema->add_name("int64_col");
  schema->add_name("float_col");
  schema->add_name("double_col");
  schema->add_name("bool_col");
  schema->add_name("string_col");
  
  // int32
  auto* col0 = response.add_arrays();
  auto* int32_arr = col0->mutable_int32_array();
  int32_arr->add_values(100);
  int32_arr->add_values(200);
  
  // int64
  auto* col1 = response.add_arrays();
  auto* int64_arr = col1->mutable_int64_array();
  int64_arr->add_values(1000);
  int64_arr->add_values(2000);
  
  // float
  auto* col2 = response.add_arrays();
  auto* float_arr = col2->mutable_float_array();
  float_arr->add_values(1.5f);
  float_arr->add_values(2.5f);
  
  // double
  auto* col3 = response.add_arrays();
  auto* double_arr = col3->mutable_double_array();
  double_arr->add_values(10.5);
  double_arr->add_values(20.5);
  
  // boolean
  auto* col4 = response.add_arrays();
  auto* bool_arr = col4->mutable_bool_array();
  bool_arr->add_values(true);
  bool_arr->add_values(false);
  
  // string
  auto* col5 = response.add_arrays();
  auto* str_arr = col5->mutable_string_array();
  str_arr->add_values("hello");
  str_arr->add_values("world");
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"int32_col", "int64_col", "float_col", "double_col", "bool_col", "string_col"};
  entry_schema->columnTypes = {createInt32Type(), createInt64Type(), createFloatType(), 
                                createDoubleType(), createBoolType(), createStringType()};
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_multi_types.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with multiple types: " << status.ToString();
  
  ASSERT_TRUE(std::filesystem::exists(export_path));
}

TEST_F(ParquetTest, TestParquetExportLargeDataset) {
  // Test export with larger dataset to verify no OOM
  neug::QueryResponse response;
  const int num_rows = 10000;
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("value");
  
  // int64 array
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < num_rows; ++i) {
    int64_arr->add_values(i);
  }
  
  // double array
  auto* col1 = response.add_arrays();
  auto* double_arr = col1->mutable_double_array();
  for (int i = 0; i < num_rows; ++i) {
    double_arr->add_values(i * 1.5);
  }
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "value"};
  entry_schema->columnTypes = {createInt64Type(), createDoubleType()};
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_large.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write large Parquet: " << status.ToString();
  
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Verify file size is reasonable (should be compressed)
  auto file_size = std::filesystem::file_size(export_path);
  EXPECT_GT(file_size, 0);
  EXPECT_LT(file_size, 1000000);  // Should be less than 1MB for 10K rows
}

TEST_F(ParquetTest, TestParquetExportWithCompressionOptions) {
  // Test export with different compression settings
  neug::QueryResponse response;
  response.set_row_count(100);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("name");
  
  // int64 array
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < 100; ++i) {
    int64_arr->add_values(i);
  }
  int64_arr->set_validity(std::string(13, 0xFF));  // All valid
  
  // string array
  auto* col1 = response.add_arrays();
  auto* str_arr = col1->mutable_string_array();
  for (int i = 0; i < 100; ++i) {
    str_arr->add_values("test_string_" + std::to_string(i));
  }
  str_arr->set_validity(std::string(13, 0xFF));
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "name"};
  entry_schema->columnTypes = {createInt64Type(), createStringType()};
  
  // Test with ZSTD compression
  std::string export_path_zstd = std::string(PARQUET_TEST_DIR) + "/export_zstd.parquet";
  reader::FileSchema file_schema_zstd;
  file_schema_zstd.paths = {export_path_zstd};
  file_schema_zstd.format = "parquet";
  file_schema_zstd.options = {{"compression", "zstd"}};
  
  auto writer_zstd = createExportWriter(file_schema_zstd, entry_schema);
  
  auto status = writer_zstd.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write ZSTD Parquet: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path_zstd));
  
  // Test with no compression
  std::string export_path_none = std::string(PARQUET_TEST_DIR) + "/export_none.parquet";
  reader::FileSchema file_schema_none;
  file_schema_none.paths = {export_path_none};
  file_schema_none.format = "parquet";
  file_schema_none.options = {{"compression", "none"}};
  
  auto writer_none = createExportWriter(file_schema_none, entry_schema);
  
  status = writer_none.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write uncompressed Parquet: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path_none));
  
  // Verify ZSTD file is smaller than uncompressed
  auto size_zstd = std::filesystem::file_size(export_path_zstd);
  auto size_none = std::filesystem::file_size(export_path_none);
  EXPECT_LT(size_zstd, size_none) << "ZSTD compressed file should be smaller than uncompressed";
  
  // Verify both files are readable
  auto sharedState_zstd = createSharedState(
      "export_zstd.parquet",
      {"id", "name"},
      {createInt64Type(), createStringType()},
      {{"batch_read", "false"}});
  
  auto reader_zstd = createParquetReader(sharedState_zstd);
  execution::Context ctx_zstd = readToContext(reader_zstd, sharedState_zstd);
  EXPECT_EQ(ctx_zstd.row_num(), 100);
  
  auto sharedState_none = createSharedState(
      "export_none.parquet",
      {"id", "name"},
      {createInt64Type(), createStringType()},
      {{"batch_read", "false"}});
  
  auto reader_none = createParquetReader(sharedState_none);
  execution::Context ctx_none = readToContext(reader_none, sharedState_none);
  EXPECT_EQ(ctx_none.row_num(), 100);
}

TEST_F(ParquetTest, TestParquetExportWithUnsupportedCompression) {
  // Test that unsupported compression codecs produce a clear error
  neug::QueryResponse response;
  response.set_row_count(3);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  int64_arr->add_values(1);
  int64_arr->add_values(2);
  int64_arr->add_values(3);
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id"};
  entry_schema->columnTypes = {createInt64Type()};
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_bad_codec.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  file_schema.options = {{"compression", "lz4"}};
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  // Should fail due to unsupported codec
  auto status = writer.writeTable(&response);
  EXPECT_FALSE(status.ok()) << "Expected failure for unsupported codec, but got OK";
}

TEST_F(ParquetTest, TestParquetExportWithRowGroupSize) {
  // Test export with custom row group size
  neug::QueryResponse response;
  response.set_row_count(100);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < 100; ++i) {
    int64_arr->add_values(i);
  }
  int64_arr->set_validity(std::string(13, 0xFF));
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id"};
  entry_schema->columnTypes = {createInt64Type()};
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_rowgroup.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  file_schema.options = {{"row_group_size", "5000"}};  // Use valid value >= 1024
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with row_group_size: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Verify file is readable
  auto sharedState = createSharedState(
      "export_rowgroup.parquet",
      {"id"},
      {createInt64Type()},
      {{"batch_read", "false"}});
  
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.row_num(), 100);
}

TEST_F(ParquetTest, TestParquetExportWithDictionaryEncoding) {
  // Test export with dictionary encoding disabled
  neug::QueryResponse response;
  const int num_rows = 10000;  // Larger dataset to show dictionary encoding benefits
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("category");
  
  // int64 array
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < num_rows; ++i) {
    int64_arr->add_values(i);
  }
  int64_arr->set_validity(std::string((num_rows + 7) / 8, 0xFF));
  
  // string array with repeated long values (good for dictionary encoding)
  // Only 5 unique values, each 100 characters long, repeated 2000 times each
  auto* col1 = response.add_arrays();
  auto* str_arr = col1->mutable_string_array();
  // Create long strings (100 chars each)
  const std::string categories[] = {
      std::string(100, 'A'),  // "AAA...A" (100 times)
      std::string(100, 'B'),  // "BBB...B" (100 times)
      std::string(100, 'C'),  // "CCC...C" (100 times)
      std::string(100, 'D'),  // "DDD...D" (100 times)
      std::string(100, 'E')   // "EEE...E" (100 times)
  };
  for (int i = 0; i < num_rows; ++i) {
    str_arr->add_values(categories[i % 5]);
  }
  str_arr->set_validity(std::string((num_rows + 7) / 8, 0xFF));
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "category"};
  entry_schema->columnTypes = {createInt64Type(), createStringType()};
  
  // Test with dictionary encoding enabled (default)
  std::string export_path_dict = std::string(PARQUET_TEST_DIR) + "/export_dict_enabled.parquet";
  reader::FileSchema file_schema_dict;
  file_schema_dict.paths = {export_path_dict};
  file_schema_dict.format = "parquet";
  file_schema_dict.options = {{"dictionary_encoding", "true"}, {"compression", "none"}};
  
  auto writer_dict = createExportWriter(file_schema_dict, entry_schema);
  
  auto status = writer_dict.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with dictionary encoding: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path_dict));
  
  // Test with dictionary encoding disabled
  std::string export_path_nodict = std::string(PARQUET_TEST_DIR) + "/export_dict_disabled.parquet";
  reader::FileSchema file_schema_nodict;
  file_schema_nodict.paths = {export_path_nodict};
  file_schema_nodict.format = "parquet";
  file_schema_nodict.options = {{"dictionary_encoding", "false"}, {"compression", "none"}};
  
  auto writer_nodict = createExportWriter(file_schema_nodict, entry_schema);
  
  status = writer_nodict.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet without dictionary encoding: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path_nodict));
  
  // With 10K rows and only 5 unique categories, dictionary encoding should be smaller
  auto size_dict = std::filesystem::file_size(export_path_dict);
  auto size_nodict = std::filesystem::file_size(export_path_nodict);
  LOG(INFO) << "Dictionary encoded size: " << size_dict << ", Non-dictionary size: " << size_nodict;
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  EXPECT_LT(size_dict, size_nodict) << "Dictionary encoded file should be smaller for low-cardinality strings";
#else
  EXPECT_GT(size_dict, 0U);
  EXPECT_GT(size_nodict, 0U);
#endif
  
  // Verify both files are readable
  auto sharedState_dict = createSharedState(
      "export_dict_enabled.parquet",
      {"id", "category"},
      {createInt64Type(), createStringType()},
      {{"batch_read", "false"}});
  
  auto reader_dict = createParquetReader(sharedState_dict);
  execution::Context ctx_dict = readToContext(reader_dict, sharedState_dict);
  EXPECT_EQ(ctx_dict.row_num(), num_rows);
  
  auto sharedState_nodict = createSharedState(
      "export_dict_disabled.parquet",
      {"id", "category"},
      {createInt64Type(), createStringType()},
      {{"batch_read", "false"}});
  
  auto reader_nodict = createParquetReader(sharedState_nodict);
  execution::Context ctx_nodict = readToContext(reader_nodict, sharedState_nodict);
  EXPECT_EQ(ctx_nodict.row_num(), num_rows);
}

TEST_F(ParquetTest, TestParquetExportWithDateAndTimestamp) {
  // Test export with date and timestamp types
  neug::QueryResponse response;
  const int num_rows = 100;
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("created_date");
  schema->add_name("updated_timestamp");
  
  // int64 array
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < num_rows; ++i) {
    int64_arr->add_values(i);
  }
  int64_arr->set_validity(std::string((num_rows + 7) / 8, 0xFF));
  
  // date array (milliseconds since epoch)
  auto* col1 = response.add_arrays();
  auto* date_arr = col1->mutable_date_array();
  for (int i = 0; i < num_rows; ++i) {
    // 2024-01-01 + i days in milliseconds
    int64_t timestamp_ms = 1704067200000LL + (i * 86400000LL);
    date_arr->add_values(timestamp_ms);
  }
  date_arr->set_validity(std::string((num_rows + 7) / 8, 0xFF));
  
  // timestamp array (microseconds since epoch)
  auto* col2 = response.add_arrays();
  auto* ts_arr = col2->mutable_timestamp_array();
  for (int i = 0; i < num_rows; ++i) {
    // 2024-01-01 00:00:00 + i seconds in microseconds
    int64_t timestamp_us = 1704067200000000LL + (i * 1000000LL);
    ts_arr->add_values(timestamp_us);
  }
  ts_arr->set_validity(std::string((num_rows + 7) / 8, 0xFF));
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_datetime.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "created_date", "updated_timestamp"};
  entry_schema->columnTypes = {createInt64Type(), createDateType(), createTimestampType()};
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with date/timestamp: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Verify file is readable
  auto sharedState = createSharedState(
      "export_datetime.parquet",
      {"id", "created_date", "updated_timestamp"},
      {createInt64Type(), createDateType(), createTimestampType()},
      {{"batch_read", "false"}});
  
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.row_num(), num_rows);
}

TEST_F(ParquetTest, TestParquetExportComprehensiveTypesRoundtrip) {
  // Mirrors comprehensive_graph/node_a row 0 scalar columns (common HTAP types).
  neug::QueryResponse response;
  response.set_row_count(1);

  auto* schema = response.mutable_schema();
  const char* column_names[] = {
      "id",           "i32_property",     "i64_property",     "u32_property",
      "u64_property", "f32_property",     "f64_property",     "str_property",
      "date_property", "datetime_property", "interval_property"};
  for (const char* name : column_names) {
    schema->add_name(name);
  }

  response.add_arrays()->mutable_int64_array()->add_values(0);
  response.add_arrays()->mutable_int32_array()->add_values(-123456789);
  response.add_arrays()->mutable_int64_array()->add_values(9223372036854775807LL);
  response.add_arrays()->mutable_uint32_array()->add_values(4294967295U);
  response.add_arrays()->mutable_uint64_array()->add_values(18446744073709551615ULL);
  response.add_arrays()->mutable_float_array()->add_values(3.1415927f);
  response.add_arrays()->mutable_double_array()->add_values(2.718281828459045);
  response.add_arrays()->mutable_string_array()->add_values("test_string_0");
  // 2023-01-15 in epoch millis
  response.add_arrays()->mutable_date_array()->add_values(1673740800000LL);
  // 2023-01-15 00:00:00 UTC in epoch micros
  response.add_arrays()->mutable_timestamp_array()->add_values(1673740800000000LL);
  response.add_arrays()->mutable_interval_array()->add_values(
      "1year2months3days4hours5minutes6seconds");

  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames.assign(std::begin(column_names),
                                     std::end(column_names));
  entry_schema->columnTypes = {
      createInt64Type(),   createInt32Type(),   createInt64Type(),
      createUint32Type(),  createUint64Type(),  createFloatType(),
      createDoubleType(),  createStringType(),  createDateType(),
      createTimestampType(), createIntervalType()};

  const std::string export_path =
      std::string(PARQUET_TEST_DIR) + "/export_comprehensive_types.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";

  auto writer = createExportWriter(file_schema, entry_schema);
  ASSERT_TRUE(writer.writeTable(&response).ok());

  auto sharedState = createSharedState(
      "export_comprehensive_types.parquet", entry_schema->columnNames,
      entry_schema->columnTypes, {{"batch_read", "false"}});
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);

  EXPECT_EQ(ctx.col_num(), 11);
  EXPECT_EQ(ctx.row_num(), 1);
}

TEST_F(ParquetTest, TestParquetExportWithListType) {
  // Test export with list/array type
  neug::QueryResponse response;
  const int num_rows = 10;
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("tags");
  
  // int64 array
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < num_rows; ++i) {
    int64_arr->add_values(i);
  }
  int64_arr->set_validity(std::string((num_rows + 7) / 8, 0xFF));
  
  // list array: each row has a list of strings
  auto* col1 = response.add_arrays();
  auto* list_arr = col1->mutable_list_array();
  
  // Add offsets: each list has 3 elements
  // For 10 rows with 3 elements each, offsets should be [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30]
  // That's 11 offset values for 10 lists
  std::vector<int32_t> offsets = {0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30};
  for (int offset : offsets) {
    list_arr->add_offsets(offset);
  }
  
  // Add string elements (30 strings total, 3 per row)
  auto* elements = list_arr->mutable_elements();
  auto* str_arr = elements->mutable_string_array();
  const char* tags[] = {"tag_A", "tag_B", "tag_C", "tag_D", "tag_E"};
  for (int i = 0; i < 30; ++i) {
    str_arr->add_values(tags[i % 5]);
  }
  str_arr->set_validity(std::string(4, 0xFF));  // All valid (30 elements need 4 bytes)
  
  // Set list validity (all valid, 10 elements need 2 bytes)
  list_arr->set_validity(std::string(2, 0xFF));
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_list.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "tags"};
  entry_schema->columnTypes = {createInt64Type(), createStringType()};  // List<String>
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with list type: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  auto file_size = std::filesystem::file_size(export_path);
  EXPECT_GT(file_size, 0) << "Parquet file should not be empty";
}

TEST_F(ParquetTest, TestParquetExportWithListOfStrings) {
  // Test export with list<string> type
  neug::QueryResponse response;
  const int num_rows = 5;
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("tags");
  
  // string array for id
  auto* col0 = response.add_arrays();
  auto* str_arr0 = col0->mutable_string_array();
  str_arr0->add_values("user_1");
  str_arr0->add_values("user_2");
  str_arr0->add_values("user_3");
  str_arr0->add_values("user_4");
  str_arr0->add_values("user_5");
  str_arr0->set_validity(std::string(1, 0xFF));
  
  // list array: each row has a list of strings with varying lengths
  auto* col1 = response.add_arrays();
  auto* list_arr = col1->mutable_list_array();
  
  // Add offsets: varying list sizes [2, 3, 1, 4, 2]
  std::vector<int32_t> offsets = {0, 2, 5, 6, 10, 12};
  for (int offset : offsets) {
    list_arr->add_offsets(offset);
  }
  
  // Add string elements (12 strings total)
  auto* elements = list_arr->mutable_elements();
  auto* str_arr = elements->mutable_string_array();
  str_arr->add_values("python");
  str_arr->add_values("java");
  str_arr->add_values("cpp");
  str_arr->add_values("go");
  str_arr->add_values("rust");
  str_arr->add_values("javascript");
  str_arr->add_values("typescript");
  str_arr->add_values("ruby");
  str_arr->add_values("php");
  str_arr->add_values("swift");
  str_arr->add_values("kotlin");
  str_arr->add_values("scala");
  str_arr->set_validity(std::string(2, 0xFF));  // All valid (12 elements)
  
  // Set list validity (all valid, 5 elements)
  list_arr->set_validity(std::string(1, 0xFF));
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_list_strings.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "tags"};
  // Note: For List<String>, the schema should indicate it's a list type
  // For now, we use String type as fallback since type inference will detect
  // it's actually a list from the proto data
  entry_schema->columnTypes = {createStringType(), createStringType()};
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with list<string>: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Verify file is non-empty
  auto file_size = std::filesystem::file_size(export_path);
  EXPECT_GT(file_size, 0) << "Parquet file should not be empty";

}

TEST_F(ParquetTest, TestParquetExportWithStructType) {
  // Test export with struct type
  neug::QueryResponse response;
  const int num_rows = 5;
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("location");
  
  // int64 array for id
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < num_rows; ++i) {
    int64_arr->add_values(i);
  }
  int64_arr->set_validity(std::string((num_rows + 7) / 8, 0xFF));
  
  // struct array: each row has a struct with {latitude: double, longitude: double}
  auto* col1 = response.add_arrays();
  auto* struct_arr = col1->mutable_struct_array();
  
  // Field 0: latitude (double)
  auto* field0 = struct_arr->add_fields();
  auto* lat_arr = field0->mutable_double_array();
  for (int i = 0; i < num_rows; ++i) {
    lat_arr->add_values(40.0 + i * 0.1);
  }
  lat_arr->set_validity(std::string(1, 0xFF));
  
  // Field 1: longitude (double)
  auto* field1 = struct_arr->add_fields();
  auto* lon_arr = field1->mutable_double_array();
  for (int i = 0; i < num_rows; ++i) {
    lon_arr->add_values(-74.0 + i * 0.1);
  }
  lon_arr->set_validity(std::string(1, 0xFF));
  
  // Struct validity (all valid)
  struct_arr->set_validity(std::string(1, 0xFF));
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_struct.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "location"};
  // Note: Schema uses string type, but type inference will detect struct from proto
  entry_schema->columnTypes = {createInt64Type(), createStringType()};
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with struct type: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Verify file is non-empty
  auto file_size = std::filesystem::file_size(export_path);
  EXPECT_GT(file_size, 0) << "Parquet file should not be empty";
  
}

TEST_F(ParquetTest, TestParquetExportWithVertexType) {
  // Test export with vertex type (JSON string parsed to struct)
  neug::QueryResponse response;
  const int num_rows = 3;
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("vertex");
  
  // int64 array for id
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < num_rows; ++i) {
    int64_arr->add_values(i + 1);
  }
  int64_arr->set_validity(std::string(1, 0xFF));
  
  // vertex array: each vertex is a JSON string
  auto* col1 = response.add_arrays();
  auto* vertex_arr = col1->mutable_vertex_array();
  
  // Add vertex JSON strings
  vertex_arr->add_values(R"({"_ID": 1, "_LABEL": "person", "fName": "Alice", "age": 30})");
  vertex_arr->add_values(R"({"_ID": 2, "_LABEL": "person", "fName": "Bob", "age": 25})");
  vertex_arr->add_values(R"({"_ID": 3, "_LABEL": "person", "fName": "Charlie", "age": 35})");
  vertex_arr->set_validity(std::string(1, 0xFF));
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_vertex.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "vertex"};
  entry_schema->columnTypes = {createInt64Type(), createStringType()};
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with vertex type: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Verify file is non-empty
  auto file_size = std::filesystem::file_size(export_path);
  EXPECT_GT(file_size, 0) << "Parquet file should not be empty";
}

TEST_F(ParquetTest, TestParquetExportWithEdgeType) {
  // Test export with edge type (JSON string parsed to struct)
  neug::QueryResponse response;
  const int num_rows = 3;
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("edge");
  
  // int64 array for id
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  for (int i = 0; i < num_rows; ++i) {
    int64_arr->add_values(i + 1);
  }
  int64_arr->set_validity(std::string(1, 0xFF));
  
  // edge array: each edge is a JSON string
  auto* col1 = response.add_arrays();
  auto* edge_arr = col1->mutable_edge_array();
  
  // Add edge JSON strings
  edge_arr->add_values(R"({"_ID": 100, "_LABEL": "knows", "_SRC_ID": 1, "_DST_ID": 2, "creationDate": "2020-01-01"})");
  edge_arr->add_values(R"({"_ID": 101, "_LABEL": "knows", "_SRC_ID": 2, "_DST_ID": 3, "creationDate": "2020-02-01"})");
  edge_arr->add_values(R"({"_ID": 102, "_LABEL": "knows", "_SRC_ID": 1, "_DST_ID": 3, "creationDate": "2020-03-01"})");
  edge_arr->set_validity(std::string(1, 0xFF));
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_edge.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "edge"};
  entry_schema->columnTypes = {createInt64Type(), createStringType()};
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with edge type: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Verify file is non-empty
  auto file_size = std::filesystem::file_size(export_path);
  EXPECT_GT(file_size, 0) << "Parquet file should not be empty";
}

TEST_F(ParquetTest, TestParquetExportWithPathType) {
  // Test export with path type (JSON string parsed to struct)
  neug::QueryResponse response;
  const int num_rows = 2;
  response.set_row_count(num_rows);
  
  auto* schema = response.mutable_schema();
  schema->add_name("id");
  schema->add_name("path");
  
  // int64 array for id
  auto* col0 = response.add_arrays();
  auto* int64_arr = col0->mutable_int64_array();
  int64_arr->add_values(1);
  int64_arr->add_values(2);
  int64_arr->set_validity(std::string(1, 0xFF));
  
  // path array: each path is a JSON string
  auto* col1 = response.add_arrays();
  auto* path_arr = col1->mutable_path_array();
  
  // Add path JSON strings
  path_arr->add_values(R"({"nodes": [{"_ID": 1, "_LABEL": "person", "fName": "Alice"}, {"_ID": 2, "_LABEL": "person", "fName": "Bob"}], "rels": [{"_ID": 100, "_LABEL": "knows", "_SRC_ID": 1, "_DST_ID": 2}], "length": 1})");
  path_arr->add_values(R"({"nodes": [{"_ID": 2, "_LABEL": "person", "fName": "Bob"}, {"_ID": 3, "_LABEL": "person", "fName": "Charlie"}], "rels": [{"_ID": 101, "_LABEL": "knows", "_SRC_ID": 2, "_DST_ID": 3}], "length": 1})");
  path_arr->set_validity(std::string(1, 0xFF));
  
  std::string export_path = std::string(PARQUET_TEST_DIR) + "/export_path.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";
  
  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "path"};
  entry_schema->columnTypes = {createInt64Type(), createStringType()};
  
  auto writer = createExportWriter(file_schema, entry_schema);
  
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << "Failed to write Parquet with path type: " << status.ToString();
  ASSERT_TRUE(std::filesystem::exists(export_path));
  
  // Verify file is non-empty
  auto file_size = std::filesystem::file_size(export_path);
  EXPECT_GT(file_size, 0) << "Parquet file should not be empty";
  
}

// =============================================================================
// Test Suite: Nested Read Tests (Carquet + Arrow)
// =============================================================================

TEST_F(ParquetTest, TestParquetListReadRoundtrip) {
  writeListParquetFile(PARQUET_TEST_DIR, "read_list.parquet");

  auto sharedState = createSharedState(
      "read_list.parquet", {"id", "tags"},
      {createInt64Type(), createListStringType()}, {{"batch_read", "false"}});

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(ctx.row_num(), 3);
}

TEST_F(ParquetTest, TestParquetInt96TimestampRead) {
  writeInt96TimestampParquetFile(PARQUET_TEST_DIR, "read_int96.parquet");

  auto sharedState = createSharedState(
      "read_int96.parquet", {"ts"}, {createTimestampType()},
      {{"batch_read", "false"}});

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.col_num(), 1);
  EXPECT_EQ(ctx.row_num(), 2);
}

TEST_F(ParquetTest, TestParquetMapReadRoundtrip) {
  writeMapParquetFile(PARQUET_TEST_DIR, "read_map.parquet");

  auto sharedState = createSharedState(
      "read_map.parquet", {"map_col"}, {createMapStringStringType()},
      {{"batch_read", "false"}});

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.col_num(), 1);
  EXPECT_EQ(ctx.row_num(), 1);

  auto map_col = ctx.chunk(0).columns()[0];
  const auto map_val = map_col->get_elem(0);
  const auto& entries = execution::ListValue::GetChildren(map_val);
  ASSERT_EQ(entries.size(), 2U);
  const auto& first_entry = execution::StructValue::GetChildren(entries[0]);
  EXPECT_EQ(first_entry[0].GetValue<std::string>(), "a");
  EXPECT_EQ(first_entry[1].GetValue<std::string>(), "abc");
}

TEST_F(ParquetTest, TestParquetStructReadRoundtrip) {
  writeStructParquetFile(PARQUET_TEST_DIR, "read_struct.parquet");

  auto sharedState = createSharedState(
      "read_struct.parquet", {"location"}, {createLocationTupleType()},
      {{"batch_read", "false"}});

  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.col_num(), 1);
  EXPECT_EQ(ctx.row_num(), 2);

  auto col = ctx.chunk(0).columns()[0];
  ASSERT_EQ(col->size(), 2U);
  auto first = col->get_elem(0);
  ASSERT_FALSE(first.IsNull());
  const auto& fields = execution::StructValue::GetChildren(first);
  ASSERT_EQ(fields.size(), 2U);
  EXPECT_DOUBLE_EQ(fields[0].GetValue<double>(), 40.0);
  EXPECT_DOUBLE_EQ(fields[1].GetValue<double>(), -74.0);
}

TEST_F(ParquetTest, TestParquetListExportRoundtrip) {
  neug::QueryResponse response;
  const int num_rows = 2;
  response.set_row_count(num_rows);

  response.mutable_schema()->add_name("id");
  response.mutable_schema()->add_name("tags");

  auto* id_arr = response.add_arrays()->mutable_int64_array();
  id_arr->add_values(1);
  id_arr->add_values(2);
  id_arr->set_validity(std::string(1, 0xFF));

  auto* list_arr = response.add_arrays()->mutable_list_array();
  list_arr->add_offsets(0);
  list_arr->add_offsets(3);
  list_arr->add_offsets(6);
  list_arr->set_validity(std::string(1, 0xFF));
  auto* elements = list_arr->mutable_elements()->mutable_string_array();
  for (const char* tag : {"tag_A", "tag_B", "tag_C", "tag_D", "tag_E", "tag_F"}) {
    elements->add_values(tag);
  }
  elements->set_validity(std::string(1, 0xFF));

  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"id", "tags"};
  entry_schema->columnTypes = {createInt64Type(), createListStringType()};

  const std::string export_path =
      std::string(PARQUET_TEST_DIR) + "/export_list_roundtrip.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";

  auto writer = createExportWriter(file_schema, entry_schema);
  ASSERT_TRUE(writer.writeTable(&response).ok());

  auto sharedState = createSharedState(
      "export_list_roundtrip.parquet", entry_schema->columnNames,
      entry_schema->columnTypes, {{"batch_read", "false"}});
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.col_num(), 2);
  EXPECT_EQ(ctx.row_num(), num_rows);

  auto tags_col = ctx.chunk(0).columns()[1];
  const auto tags0 = tags_col->get_elem(0);
  const auto& tags0_elems = execution::ListValue::GetChildren(tags0);
  ASSERT_EQ(tags0_elems.size(), 3U);
  EXPECT_EQ(tags0_elems[0].GetValue<std::string>(), "tag_A");
  EXPECT_EQ(tags0_elems[1].GetValue<std::string>(), "tag_B");
  EXPECT_EQ(tags0_elems[2].GetValue<std::string>(), "tag_C");
}

TEST_F(ParquetTest, TestParquetMapExportRoundtrip) {
  neug::QueryResponse response;
  response.set_row_count(1);

  response.mutable_schema()->add_name("map_col");
  appendMapListColumn(response.add_arrays(), 2, {"a", "b"}, {"abc", "bcd"});

  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"map_col"};
  entry_schema->columnTypes = {createMapStringStringType()};

  const std::string export_path =
      std::string(PARQUET_TEST_DIR) + "/export_map_roundtrip.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";

  auto writer = createExportWriter(file_schema, entry_schema);
  ASSERT_TRUE(writer.writeTable(&response).ok());

  {
    auto infer_state = createSharedState(
        "export_map_roundtrip.parquet", {}, {}, {{"batch_read", "false"}});
    auto infer_reader = createParquetReader(infer_state);
    auto inferred = infer_reader->inferSchema();
    ASSERT_TRUE(inferred.has_value())
        << (inferred.has_value() ? "" : inferred.error().ToString());
    ASSERT_EQ(inferred.value()->columnTypes.size(), 1U);
    EXPECT_EQ(inferred.value()->columnTypes[0]->item_case(),
              ::common::DataType::kMap);
  }

  auto sharedState = createSharedState(
      "export_map_roundtrip.parquet", entry_schema->columnNames,
      entry_schema->columnTypes, {{"batch_read", "false"}});
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.col_num(), 1);
  EXPECT_EQ(ctx.row_num(), 1);

  auto map_col = ctx.chunk(0).columns()[0];
  ASSERT_EQ(map_col->size(), 1U);
  const auto map_val = map_col->get_elem(0);
  const auto& entries = execution::ListValue::GetChildren(map_val);
  ASSERT_EQ(entries.size(), 2U);
  const auto& first_entry = execution::StructValue::GetChildren(entries[0]);
  EXPECT_EQ(first_entry[0].GetValue<std::string>(), "a");
  EXPECT_EQ(first_entry[1].GetValue<std::string>(), "abc");
}

TEST_F(ParquetTest, TestParquetStructExportRoundtrip) {
  neug::QueryResponse response;
  const int num_rows = 2;
  response.set_row_count(num_rows);

  response.mutable_schema()->add_name("location");
  auto* struct_arr = response.add_arrays()->mutable_struct_array();
  auto* lat_arr = struct_arr->add_fields()->mutable_double_array();
  auto* lon_arr = struct_arr->add_fields()->mutable_double_array();
  lat_arr->add_values(40.0);
  lat_arr->add_values(41.0);
  lon_arr->add_values(-74.0);
  lon_arr->add_values(-73.0);
  lat_arr->set_validity(std::string(1, 0xFF));
  lon_arr->set_validity(std::string(1, 0xFF));
  struct_arr->set_validity(std::string(1, 0xFF));

  auto entry_schema = std::make_shared<reader::TableEntrySchema>();
  entry_schema->columnNames = {"location"};
  entry_schema->columnTypes = {createLocationTupleType()};

  const std::string export_path =
      std::string(PARQUET_TEST_DIR) + "/export_struct_roundtrip.parquet";
  reader::FileSchema file_schema;
  file_schema.paths = {export_path};
  file_schema.format = "parquet";

  auto writer = createExportWriter(file_schema, entry_schema);
  ASSERT_TRUE(writer.writeTable(&response).ok());

  auto sharedState = createSharedState(
      "export_struct_roundtrip.parquet", entry_schema->columnNames,
      entry_schema->columnTypes, {{"batch_read", "false"}});
  auto reader = createParquetReader(sharedState);
  execution::Context ctx = readToContext(reader, sharedState);
  EXPECT_EQ(ctx.col_num(), 1);
  EXPECT_EQ(ctx.row_num(), num_rows);

  auto col = ctx.chunk(0).columns()[0];
  auto val = col->get_elem(0);
  const auto& fields = execution::StructValue::GetChildren(val);
  ASSERT_EQ(fields.size(), 2U);
  EXPECT_DOUBLE_EQ(fields[0].GetValue<double>(), 40.0);
  EXPECT_DOUBLE_EQ(fields[1].GetValue<double>(), -74.0);
}

// =============================================================================
// Test Suite: Schema Validation
// Verify reader-level column existence check
// =============================================================================

TEST_F(ParquetTest, TestParquetNonExistentColumnThrows) {
  createSimpleParquetFile("test_nonexist.parquet");

  std::vector<std::string> columnNames = {"id", "name", "wrong_col"};
  std::vector<std::shared_ptr<::common::DataType>> columnTypes = {
      createInt64Type(), createStringType(), createDoubleType()};

  auto sharedState = createSharedState(
      "test_nonexist.parquet", columnNames, columnTypes,
      {{"batch_read", "false"}});
  auto reader = createParquetReader(sharedState);

  EXPECT_THROW(readToContext(reader, sharedState),
               exception::SchemaMismatchException);
}

// End of Test Suites

}  // namespace test
}  // namespace neug
