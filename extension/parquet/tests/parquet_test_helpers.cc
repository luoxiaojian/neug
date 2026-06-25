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

#include "parquet_test_helpers.h"

#include <string>
#include <vector>

#if !(defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW)
#include <cstring>
#endif

#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#else
#include <carquet/carquet.h>
#endif

namespace neug {
namespace test {
namespace {

std::string joinPath(const std::string& directory, const std::string& filename) {
  return directory + "/" + filename;
}

#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW

void writeTableToParquet(const std::string& filepath,
                         const std::shared_ptr<arrow::Table>& table,
                         int64_t row_group_size = 3) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile, row_group_size));
}

#endif

}  // namespace

void writeSimpleParquetFile(const std::string& directory,
                            const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema = arrow::schema({arrow::field("id", arrow::int64()),
                               arrow::field("name", arrow::utf8()),
                               arrow::field("value", arrow::float64())});
  arrow::Int64Builder id_builder;
  arrow::StringBuilder name_builder;
  arrow::DoubleBuilder value_builder;
  ASSERT_TRUE(id_builder.AppendValues({1, 2, 3}).ok());
  ASSERT_TRUE(name_builder.AppendValues({"Alice", "Bob", "Charlie"}).ok());
  ASSERT_TRUE(value_builder.AppendValues({10.5, 20.3, 30.7}).ok());
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> name_array;
  std::shared_ptr<arrow::Array> value_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(name_builder.Finish(&name_array).ok());
  ASSERT_TRUE(value_builder.Finish(&value_array).ok());
  writeTableToParquet(
      filepath, arrow::Table::Make(schema, {id_array, name_array, value_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  ASSERT_EQ(carquet_schema_add_column(schema, "name", CARQUET_PHYSICAL_BYTE_ARRAY,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  ASSERT_EQ(carquet_schema_add_column(schema, "value", CARQUET_PHYSICAL_DOUBLE,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const int64_t ids[] = {1, 2, 3};
  const carquet_byte_array_t names[] = {{(uint8_t*)"Alice", 5},
                                        {(uint8_t*)"Bob", 3},
                                        {(uint8_t*)"Charlie", 7}};
  const double values[] = {10.5, 20.3, 30.7};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, ids, 3, nullptr, nullptr),
            CARQUET_OK);
  ASSERT_EQ(
      carquet_writer_write_batch(writer, 1, names, 3, nullptr, nullptr),
      CARQUET_OK);
  ASSERT_EQ(
      carquet_writer_write_batch(writer, 2, values, 3, nullptr, nullptr),
      CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeNumericTypesParquetFile(const std::string& directory,
                                  const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema = arrow::schema(
      {arrow::field("int32_col", arrow::int32()),
       arrow::field("int64_col", arrow::int64()),
       arrow::field("double_col", arrow::float64()),
       arrow::field("bool_col", arrow::boolean())});
  arrow::Int32Builder int32_builder;
  arrow::Int64Builder int64_builder;
  arrow::DoubleBuilder double_builder;
  arrow::BooleanBuilder bool_builder;
  ASSERT_TRUE(int32_builder.Append(42).ok());
  ASSERT_TRUE(int64_builder.Append(9223372036854775807LL).ok());
  ASSERT_TRUE(double_builder.Append(3.14159).ok());
  ASSERT_TRUE(bool_builder.Append(true).ok());
  std::shared_ptr<arrow::Array> arrays[4];
  ASSERT_TRUE(int32_builder.Finish(&arrays[0]).ok());
  ASSERT_TRUE(int64_builder.Finish(&arrays[1]).ok());
  ASSERT_TRUE(double_builder.Finish(&arrays[2]).ok());
  ASSERT_TRUE(bool_builder.Finish(&arrays[3]).ok());
  writeTableToParquet(
      filepath,
      arrow::Table::Make(schema, {arrays[0], arrays[1], arrays[2], arrays[3]}),
      1);
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(carquet_schema_add_column(schema, "int32_col", CARQUET_PHYSICAL_INT32,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  ASSERT_EQ(carquet_schema_add_column(schema, "int64_col", CARQUET_PHYSICAL_INT64,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  ASSERT_EQ(
      carquet_schema_add_column(schema, "double_col", CARQUET_PHYSICAL_DOUBLE,
                                nullptr, CARQUET_REPETITION_REQUIRED, 0, 0),
      CARQUET_OK);
  ASSERT_EQ(carquet_schema_add_column(schema, "bool_col", CARQUET_PHYSICAL_BOOLEAN,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const int32_t i32 = 42;
  const int64_t i64 = 9223372036854775807LL;
  const double dbl = 3.14159;
  const uint8_t flag = 1;
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, &i32, 1, nullptr, nullptr),
            CARQUET_OK);
  ASSERT_EQ(carquet_writer_write_batch(writer, 1, &i64, 1, nullptr, nullptr),
            CARQUET_OK);
  ASSERT_EQ(carquet_writer_write_batch(writer, 2, &dbl, 1, nullptr, nullptr),
            CARQUET_OK);
  ASSERT_EQ(carquet_writer_write_batch(writer, 3, &flag, 1, nullptr, nullptr),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writePruningParquetFile(const std::string& directory,
                             const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema =
      arrow::schema({arrow::field("id", arrow::int32()),
                     arrow::field("name", arrow::utf8()),
                     arrow::field("score", arrow::float64()),
                     arrow::field("grade", arrow::utf8())});
  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder;
  arrow::DoubleBuilder score_builder;
  arrow::StringBuilder grade_builder;
  ASSERT_TRUE(id_builder.Append(1).ok());
  ASSERT_TRUE(name_builder.Append("Alice").ok());
  ASSERT_TRUE(score_builder.Append(95.5).ok());
  ASSERT_TRUE(grade_builder.Append("A").ok());
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> name_array;
  std::shared_ptr<arrow::Array> score_array;
  std::shared_ptr<arrow::Array> grade_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(name_builder.Finish(&name_array).ok());
  ASSERT_TRUE(score_builder.Finish(&score_array).ok());
  ASSERT_TRUE(grade_builder.Finish(&grade_array).ok());
  writeTableToParquet(
      filepath,
      arrow::Table::Make(schema, {id_array, name_array, score_array, grade_array}),
      1);
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT32, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_schema_add_column(schema, "name", CARQUET_PHYSICAL_BYTE_ARRAY, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_schema_add_column(schema, "score", CARQUET_PHYSICAL_DOUBLE, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_schema_add_column(schema, "grade", CARQUET_PHYSICAL_BYTE_ARRAY, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const int32_t id = 1;
  const carquet_byte_array_t name = {(uint8_t*)"Alice", 5};
  const double score = 95.5;
  const carquet_byte_array_t grade = {(uint8_t*)"A", 1};
  carquet_writer_write_batch(writer, 0, &id, 1, nullptr, nullptr);
  carquet_writer_write_batch(writer, 1, &name, 1, nullptr, nullptr);
  carquet_writer_write_batch(writer, 2, &score, 1, nullptr, nullptr);
  carquet_writer_write_batch(writer, 3, &grade, 1, nullptr, nullptr);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeFilterParquetFile(const std::string& directory,
                            const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                               arrow::field("score", arrow::float64())});
  arrow::Int32Builder id_builder;
  arrow::DoubleBuilder score_builder;
  ASSERT_TRUE(id_builder.AppendValues({1, 2, 3, 4, 5}).ok());
  ASSERT_TRUE(score_builder.AppendValues({95.5, 87.0, 92.5, 78.0, 98.0}).ok());
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> score_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(score_builder.Finish(&score_array).ok());
  writeTableToParquet(filepath,
                      arrow::Table::Make(schema, {id_array, score_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT32, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_schema_add_column(schema, "score", CARQUET_PHYSICAL_DOUBLE, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const int32_t ids[] = {1, 2, 3, 4, 5};
  const double scores[] = {95.5, 87.0, 92.5, 78.0, 98.0};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, ids, 5, nullptr, nullptr),
            CARQUET_OK);
  ASSERT_EQ(carquet_writer_write_batch(writer, 1, scores, 5, nullptr, nullptr),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeBatchModeParquetFile(const std::string& directory,
                               const std::string& filename) {
  writeSimpleParquetFile(directory, filename);
}

void writeCombinedParquetFile(const std::string& directory,
                              const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema =
      arrow::schema({arrow::field("id", arrow::int32()),
                     arrow::field("name", arrow::utf8()),
                     arrow::field("score", arrow::float64()),
                     arrow::field("grade", arrow::utf8())});
  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder;
  arrow::DoubleBuilder score_builder;
  arrow::StringBuilder grade_builder;
  ASSERT_TRUE(id_builder.AppendValues({1, 2, 3, 4}).ok());
  ASSERT_TRUE(name_builder.AppendValues({"A", "B", "C", "D"}).ok());
  ASSERT_TRUE(score_builder.AppendValues({85.0, 92.0, 95.0, 70.0}).ok());
  ASSERT_TRUE(grade_builder.AppendValues({"B", "A", "A", "C"}).ok());
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> name_array;
  std::shared_ptr<arrow::Array> score_array;
  std::shared_ptr<arrow::Array> grade_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(name_builder.Finish(&name_array).ok());
  ASSERT_TRUE(score_builder.Finish(&score_array).ok());
  ASSERT_TRUE(grade_builder.Finish(&grade_array).ok());
  writeTableToParquet(
      filepath,
      arrow::Table::Make(schema, {id_array, name_array, score_array, grade_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT32, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_schema_add_column(schema, "name", CARQUET_PHYSICAL_BYTE_ARRAY, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_schema_add_column(schema, "score", CARQUET_PHYSICAL_DOUBLE, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_schema_add_column(schema, "grade", CARQUET_PHYSICAL_BYTE_ARRAY, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const int32_t ids[] = {1, 2, 3, 4};
  const carquet_byte_array_t names[] = {{(uint8_t*)"A", 1},
                                        {(uint8_t*)"B", 1},
                                        {(uint8_t*)"C", 1},
                                        {(uint8_t*)"D", 1}};
  const double scores[] = {85.0, 92.0, 95.0, 70.0};
  const carquet_byte_array_t grades[] = {{(uint8_t*)"B", 1},
                                         {(uint8_t*)"A", 1},
                                         {(uint8_t*)"A", 1},
                                         {(uint8_t*)"C", 1}};
  carquet_writer_write_batch(writer, 0, ids, 4, nullptr, nullptr);
  carquet_writer_write_batch(writer, 1, names, 4, nullptr, nullptr);
  carquet_writer_write_batch(writer, 2, scores, 4, nullptr, nullptr);
  carquet_writer_write_batch(writer, 3, grades, 4, nullptr, nullptr);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeMultiFileParquet(const std::string& directory,
                           const std::string& filename, int start_id,
                           int count) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema = arrow::schema({arrow::field("id", arrow::int64())});
  arrow::Int64Builder id_builder;
  for (int i = 0; i < count; ++i) {
    ASSERT_TRUE(id_builder.Append(start_id + i).ok());
  }
  std::shared_ptr<arrow::Array> id_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  writeTableToParquet(filepath, arrow::Table::Make(schema, {id_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64, nullptr,
                            CARQUET_REPETITION_REQUIRED, 0, 0);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  std::vector<int64_t> ids(count);
  for (int i = 0; i < count; ++i) {
    ids[static_cast<size_t>(i)] = start_id + i;
  }
  carquet_writer_write_batch(writer, 0, ids.data(), count, nullptr, nullptr);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeListParquetFile(const std::string& directory,
                          const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema = arrow::schema(
      {arrow::field("id", arrow::int64()),
       arrow::field("tags", arrow::list(arrow::utf8()))});
  arrow::Int64Builder id_builder;
  ASSERT_TRUE(id_builder.AppendValues({1, 2, 3}).ok());
  arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                  std::make_unique<arrow::StringBuilder>());
  auto* str_builder =
      static_cast<arrow::StringBuilder*>(list_builder.value_builder());
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("tag_A").ok());
  ASSERT_TRUE(str_builder->Append("tag_B").ok());
  ASSERT_TRUE(str_builder->Append("tag_C").ok());
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("tag_D").ok());
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("tag_E").ok());
  ASSERT_TRUE(str_builder->Append("tag_F").ok());
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> list_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(list_builder.Finish(&list_array).ok());
  writeTableToParquet(filepath,
                      arrow::Table::Make(schema, {id_array, list_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  ASSERT_GE(carquet_schema_add_list(schema, "tags", CARQUET_PHYSICAL_BYTE_ARRAY,
                                    nullptr, CARQUET_REPETITION_OPTIONAL, 0, 0),
            0);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const int64_t ids[] = {1, 2, 3};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, ids, 3, nullptr, nullptr),
            CARQUET_OK);
  const carquet_byte_array_t tags[] = {
      {(uint8_t*)"tag_A", 5}, {(uint8_t*)"tag_B", 3}, {(uint8_t*)"tag_C", 3},
      {(uint8_t*)"tag_D", 3}, {(uint8_t*)"tag_E", 3}, {(uint8_t*)"tag_F", 3}};
  const int16_t tag_def[] = {3, 3, 3, 3, 3, 3};
  const int16_t tag_rep[] = {0, 1, 1, 0, 0, 1};
  ASSERT_EQ(carquet_writer_write_batch(writer, 1, tags, 6, tag_def, tag_rep),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeMapParquetFile(const std::string& directory,
                         const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto map_type = arrow::map(arrow::utf8(), arrow::utf8());
  auto schema = arrow::schema({arrow::field("map_col", map_type)});
  arrow::MapBuilder map_builder(arrow::default_memory_pool(),
                                std::make_unique<arrow::StringBuilder>(),
                                std::make_unique<arrow::StringBuilder>());
  auto* key_builder =
      static_cast<arrow::StringBuilder*>(map_builder.key_builder());
  auto* item_builder =
      static_cast<arrow::StringBuilder*>(map_builder.item_builder());
  ASSERT_TRUE(map_builder.Append().ok());
  ASSERT_TRUE(key_builder->Append("a").ok());
  ASSERT_TRUE(item_builder->Append("abc").ok());
  ASSERT_TRUE(key_builder->Append("b").ok());
  ASSERT_TRUE(item_builder->Append("bcd").ok());
  std::shared_ptr<arrow::Array> map_array;
  ASSERT_TRUE(map_builder.Finish(&map_array).ok());
  writeTableToParquet(filepath, arrow::Table::Make(schema, {map_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  carquet_logical_type_t string_logical{};
  string_logical.id = CARQUET_LOGICAL_STRING;
  ASSERT_GE(carquet_schema_add_map(schema, "map_col", CARQUET_PHYSICAL_BYTE_ARRAY,
                                   &string_logical, 0, CARQUET_PHYSICAL_BYTE_ARRAY,
                                   &string_logical, 0, CARQUET_REPETITION_OPTIONAL,
                                   0),
            0);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const carquet_byte_array_t keys[] = {{(uint8_t*)"a", 1}, {(uint8_t*)"b", 1}};
  const int16_t key_def[] = {2, 2};
  const int16_t key_rep[] = {0, 1};
  const carquet_byte_array_t vals[] = {{(uint8_t*)"abc", 3}, {(uint8_t*)"bcd", 3}};
  const int16_t val_def[] = {3, 3};
  const int16_t val_rep[] = {0, 1};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, keys, 2, key_def, key_rep),
            CARQUET_OK);
  ASSERT_EQ(carquet_writer_write_batch(writer, 1, vals, 2, val_def, val_rep),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeStructParquetFile(const std::string& directory,
                            const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto struct_type = arrow::struct_(
      {arrow::field("field_0", arrow::float64()),
       arrow::field("field_1", arrow::float64())});
  auto schema = arrow::schema({arrow::field("location", struct_type)});
  arrow::DoubleBuilder lat_builder;
  arrow::DoubleBuilder lon_builder;
  ASSERT_TRUE(lat_builder.AppendValues({40.0, 41.0}).ok());
  ASSERT_TRUE(lon_builder.AppendValues({-74.0, -73.0}).ok());
  std::shared_ptr<arrow::Array> lat_array;
  std::shared_ptr<arrow::Array> lon_array;
  ASSERT_TRUE(lat_builder.Finish(&lat_array).ok());
  ASSERT_TRUE(lon_builder.Finish(&lon_array).ok());
  auto struct_array = std::make_shared<arrow::StructArray>(
      struct_type, 2, std::vector<std::shared_ptr<arrow::Array>>{lat_array,
                                                                 lon_array});
  writeTableToParquet(filepath, arrow::Table::Make(schema, {struct_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  const int32_t group_idx = carquet_schema_add_group(
      schema, "location", CARQUET_REPETITION_OPTIONAL, 0);
  ASSERT_GE(group_idx, 0);
  carquet_logical_type_t string_logical{};
  (void)string_logical;
  ASSERT_EQ(carquet_schema_add_column(schema, "field_0", CARQUET_PHYSICAL_DOUBLE,
                                      nullptr, CARQUET_REPETITION_OPTIONAL, 0,
                                      group_idx),
            CARQUET_OK);
  ASSERT_EQ(carquet_schema_add_column(schema, "field_1", CARQUET_PHYSICAL_DOUBLE,
                                      nullptr, CARQUET_REPETITION_OPTIONAL, 0,
                                      group_idx),
            CARQUET_OK);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const double lats[] = {40.0, 41.0};
  const double lons[] = {-74.0, -73.0};
  const int16_t def[] = {2, 2};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, lats, 2, def, nullptr),
            CARQUET_OK);
  ASSERT_EQ(carquet_writer_write_batch(writer, 1, lons, 2, def, nullptr),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeInt96TimestampParquetFile(const std::string& directory,
                                    const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema = arrow::schema({arrow::field("ts", arrow::timestamp(arrow::TimeUnit::NANO))});
  arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO),
                                  arrow::default_memory_pool());
  ASSERT_TRUE(builder.Append(1704067200000000000LL).ok());
  ASSERT_TRUE(builder.Append(1704153600000000000LL).ok());
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());
  writeTableToParquet(filepath, arrow::Table::Make(schema, {array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(carquet_schema_add_column(schema, "ts", CARQUET_PHYSICAL_INT96,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  auto to_int96 = [](int64_t millis) {
    carquet_int96_t value{};
    const int64_t days = millis / 86400000LL;
    const int64_t day_millis = millis % 86400000LL;
    const int64_t nanos = day_millis * 1000000LL;
    std::memcpy(value.value, &nanos, sizeof(int64_t));
    value.value[2] = static_cast<uint32_t>(days + 2440588LL);
    return value;
  };
  const carquet_int96_t values[] = {to_int96(1704067200000LL),
                                    to_int96(1704153600000LL)};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, values, 2, nullptr, nullptr),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeMultiListParquetFile(const std::string& directory,
                               const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema = arrow::schema(
      {arrow::field("id", arrow::int64()),
       arrow::field("tags", arrow::list(arrow::utf8()))});
  arrow::Int64Builder id_builder;
  ASSERT_TRUE(id_builder.AppendValues({1, 2, 3}).ok());
  arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                  std::make_unique<arrow::StringBuilder>());
  auto* str_builder =
      static_cast<arrow::StringBuilder*>(list_builder.value_builder());
  // Row 1: ["A", "B", "C"]
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("A").ok());
  ASSERT_TRUE(str_builder->Append("B").ok());
  ASSERT_TRUE(str_builder->Append("C").ok());
  // Row 2: ["D"]
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("D").ok());
  // Row 3: ["E", "F"]
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("E").ok());
  ASSERT_TRUE(str_builder->Append("F").ok());
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> list_array;
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(list_builder.Finish(&list_array).ok());
  writeTableToParquet(filepath,
                      arrow::Table::Make(schema, {id_array, list_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  ASSERT_GE(carquet_schema_add_list(schema, "tags", CARQUET_PHYSICAL_BYTE_ARRAY,
                                    nullptr, CARQUET_REPETITION_OPTIONAL, 0, 0),
            0);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  const int64_t ids[] = {1, 2, 3};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, ids, 3, nullptr, nullptr),
            CARQUET_OK);
  // values buffer contains packed non-null elements: A, B, C, D, E, F
  const carquet_byte_array_t tags[] = {
      {(uint8_t*)"A", 1}, {(uint8_t*)"B", 1}, {(uint8_t*)"C", 1},
      {(uint8_t*)"D", 1}, {(uint8_t*)"E", 1}, {(uint8_t*)"F", 1}};
  // def_levels: 3 = list present + element present
  const int16_t tag_def[] = {3, 3, 3, 3, 3, 3};
  // rep_levels: 0 = first element in list, 1 = continuation
  const int16_t tag_rep[] = {0, 1, 1, 0, 0, 1};
  ASSERT_EQ(carquet_writer_write_batch(writer, 1, tags, 6, tag_def, tag_rep),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeMixedColumnOrderParquetFile(const std::string& directory,
                                      const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto schema = arrow::schema(
      {arrow::field("tags", arrow::list(arrow::utf8())),
       arrow::field("id", arrow::int64()),
       arrow::field("name", arrow::utf8())});
  arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                  std::make_unique<arrow::StringBuilder>());
  auto* str_builder =
      static_cast<arrow::StringBuilder*>(list_builder.value_builder());
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("x").ok());
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("y").ok());
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(str_builder->Append("z").ok());
  arrow::Int64Builder id_builder;
  ASSERT_TRUE(id_builder.AppendValues({10, 20, 30}).ok());
  arrow::StringBuilder name_builder;
  ASSERT_TRUE(name_builder.AppendValues({"foo", "bar", "baz"}).ok());
  std::shared_ptr<arrow::Array> list_array;
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> name_array;
  ASSERT_TRUE(list_builder.Finish(&list_array).ok());
  ASSERT_TRUE(id_builder.Finish(&id_array).ok());
  ASSERT_TRUE(name_builder.Finish(&name_array).ok());
  writeTableToParquet(
      filepath,
      arrow::Table::Make(schema, {list_array, id_array, name_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  // Column 0: tags (list<string>) — added first
  ASSERT_GE(carquet_schema_add_list(schema, "tags", CARQUET_PHYSICAL_BYTE_ARRAY,
                                    nullptr, CARQUET_REPETITION_OPTIONAL, 0, 0),
            0);
  // Column 1: id (int64) — added second
  ASSERT_EQ(carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  // Column 2: name (string) — added third
  ASSERT_EQ(carquet_schema_add_column(schema, "name", CARQUET_PHYSICAL_BYTE_ARRAY,
                                      nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                      0),
            CARQUET_OK);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  // tags: [["x"], ["y"], ["z"]]
  const carquet_byte_array_t tags[] = {
      {(uint8_t*)"x", 1}, {(uint8_t*)"y", 1}, {(uint8_t*)"z", 1}};
  const int16_t tag_def[] = {3, 3, 3};
  const int16_t tag_rep[] = {0, 0, 0};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, tags, 3, tag_def, tag_rep),
            CARQUET_OK);
  const int64_t ids[] = {10, 20, 30};
  ASSERT_EQ(carquet_writer_write_batch(writer, 1, ids, 3, nullptr, nullptr),
            CARQUET_OK);
  const carquet_byte_array_t names[] = {
      {(uint8_t*)"foo", 3}, {(uint8_t*)"bar", 3}, {(uint8_t*)"baz", 3}};
  ASSERT_EQ(carquet_writer_write_batch(writer, 2, names, 3, nullptr, nullptr),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

void writeMapWithNullValueParquetFile(const std::string& directory,
                                      const std::string& filename) {
  const std::string filepath = joinPath(directory, filename);
#if defined(NEUG_PARQUET_USE_ARROW) && NEUG_PARQUET_USE_ARROW
  auto map_type = arrow::map(arrow::utf8(), arrow::utf8());
  auto schema = arrow::schema({arrow::field("map_col", map_type)});
  arrow::MapBuilder map_builder(arrow::default_memory_pool(),
                                std::make_unique<arrow::StringBuilder>(),
                                std::make_unique<arrow::StringBuilder>());
  auto* key_builder =
      static_cast<arrow::StringBuilder*>(map_builder.key_builder());
  auto* item_builder =
      static_cast<arrow::StringBuilder*>(map_builder.item_builder());
  // Row 1: {"a": "abc", "b": null}
  ASSERT_TRUE(map_builder.Append().ok());
  ASSERT_TRUE(key_builder->Append("a").ok());
  ASSERT_TRUE(item_builder->Append("abc").ok());
  ASSERT_TRUE(key_builder->Append("b").ok());
  ASSERT_TRUE(item_builder->AppendNull().ok());
  std::shared_ptr<arrow::Array> map_array;
  ASSERT_TRUE(map_builder.Finish(&map_array).ok());
  writeTableToParquet(filepath, arrow::Table::Make(schema, {map_array}));
#else
  carquet_error_t err = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&err);
  ASSERT_NE(schema, nullptr);
  carquet_logical_type_t string_logical{};
  string_logical.id = CARQUET_LOGICAL_STRING;
  ASSERT_GE(carquet_schema_add_map(schema, "map_col", CARQUET_PHYSICAL_BYTE_ARRAY,
                                   &string_logical, 0, CARQUET_PHYSICAL_BYTE_ARRAY,
                                   &string_logical, 0, CARQUET_REPETITION_OPTIONAL,
                                   0),
            0);
  carquet_writer_options_t opts;
  carquet_writer_options_init(&opts);
  carquet_writer_t* writer =
      carquet_writer_create(filepath.c_str(), schema, &opts, &err);
  ASSERT_NE(writer, nullptr);
  // Keys: 2 entries, both present (key_max_def = 2)
  const carquet_byte_array_t keys[] = {{(uint8_t*)"a", 1}, {(uint8_t*)"b", 1}};
  const int16_t key_def[] = {2, 2};
  const int16_t key_rep[] = {0, 1};
  // Values: 2 logical slots, but only 1 non-null value (packed buffer)
  // val_max_def = 3; def=3 means value present, def=2 means value null
  const carquet_byte_array_t vals[] = {{(uint8_t*)"abc", 3}};
  const int16_t val_def[] = {3, 2};
  const int16_t val_rep[] = {0, 1};
  ASSERT_EQ(carquet_writer_write_batch(writer, 0, keys, 2, key_def, key_rep),
            CARQUET_OK);
  ASSERT_EQ(carquet_writer_write_batch(writer, 1, vals, 2, val_def, val_rep),
            CARQUET_OK);
  carquet_writer_close(writer);
  carquet_schema_free(schema);
#endif
}

}  // namespace test
}  // namespace neug
