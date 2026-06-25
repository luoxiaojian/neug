# Parquet extension sources (explicit list; backends are mutually exclusive at build time).
set(_PARQUET_COMMON_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet_decoder.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet_reader.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet_sniffer.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet_options.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet_extension.cpp)

set(_PARQUET_NATIVE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/native_parquet_decoder.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/carquet_type_converter.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/carquet_value_converter.cc)

set(_PARQUET_ARROW_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_parquet_decoder.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_context_column.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_type_converter.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_random_access_file.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_fs_resolver.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/record_batch_supplier.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet_export_function.cc)

set(PARQUET_EXTENSION_SOURCES ${_PARQUET_COMMON_SOURCES})
if(PARQUET_USE_ARROW)
    list(APPEND PARQUET_EXTENSION_SOURCES ${_PARQUET_ARROW_SOURCES})
else()
    list(APPEND PARQUET_EXTENSION_SOURCES ${_PARQUET_NATIVE_SOURCES})
endif()
set(PARQUET_EXTENSION_OBJECT_FILES ${PARQUET_EXTENSION_SOURCES})

build_extension_lib("parquet")

target_compile_definitions(neug_parquet_extension PRIVATE
    NEUG_PARQUET_USE_ARROW=$<BOOL:${PARQUET_USE_ARROW}>)

target_include_directories(neug_parquet_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

if(PARQUET_USE_ARROW AND ARROW_INCLUDE_DIRS)
    target_include_directories(neug_parquet_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()

if(PARQUET_USE_ARROW)
    if(TARGET parquet_objlib)
        set(_EXT_ARROW_PARQUET_LIB parquet_objlib)
    elseif(TARGET arrow_parquet_objlib)
        set(_EXT_ARROW_PARQUET_LIB arrow_parquet_objlib)
    else()
        set(_EXT_ARROW_PARQUET_LIB ${ARROW_PARQUET_LIB})
    endif()

    set(_PARQUET_EXTRA_LIBS "")
    if(ARROW_ENABLE_S3)
        find_package(OpenSSL REQUIRED)
        set(_PARQUET_EXTRA_LIBS
            $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
            OpenSSL::SSL
            OpenSSL::Crypto
            $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>)
    endif()

    target_link_libraries(neug_parquet_extension PRIVATE
        ${ARROW_BASE_LIB}
        ${_EXT_ARROW_PARQUET_LIB}
        ${ARROW_BUNDLED_DEPS_LIB}
        ${_PARQUET_EXTRA_LIBS}
        ${Protobuf_LIBRARIES}
        neug)
else()
    target_link_libraries(neug_parquet_extension PRIVATE carquet neug)
endif()

if(BUILD_TEST)
    set(_PARQUET_TEST_EXTRA_LIBS "")
    if(PARQUET_USE_ARROW)
        list(APPEND _PARQUET_TEST_EXTRA_LIBS ${ARROW_PARQUET_LIB})
    endif()
    add_extension_test(NAME parquet
        TEST_SRCS
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/parquet_test.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/parquet_test_helpers.cc
        EXTRA_LINK_LIBS ${_PARQUET_TEST_EXTRA_LIBS}
    )
    if(PARQUET_USE_ARROW AND ARROW_INCLUDE_DIRS)
        target_include_directories(parquet_extension_test PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
    endif()
    target_compile_definitions(parquet_extension_test PRIVATE
        NEUG_PARQUET_USE_ARROW=$<BOOL:${PARQUET_USE_ARROW}>)
    if(NOT PARQUET_USE_ARROW)
        target_link_libraries(parquet_extension_test PRIVATE carquet)
    endif()
endif()
