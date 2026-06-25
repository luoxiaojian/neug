# Collect source files
file(GLOB PARQUET_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

set(PARQUET_EXTENSION_OBJECT_FILES ${PARQUET_EXTENSION_SOURCES})

# Use the extension build system (should be called before add_subdirectory(test))
build_extension_lib("parquet")

# Set include directories
target_include_directories(neug_parquet_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

if(ARROW_INCLUDE_DIRS)
    target_include_directories(neug_parquet_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()

# Link only libarrow core + libparquet (no Dataset/Acero/Compute).
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
    neug
    ${Protobuf_LIBRARIES}
)

if(BUILD_TEST)
    add_extension_test(NAME parquet
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/parquet_test.cpp
        EXTRA_LINK_LIBS ${ARROW_PARQUET_LIB}
    )
endif()
