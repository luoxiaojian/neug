# Copyright 2020 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)

set(FC_DECLARE_COMMON_OPTIONS)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    list(APPEND FC_DECLARE_COMMON_OPTIONS EXCLUDE_FROM_ALL)
endif()

function(build_carquet_as_third_party)
    if(TARGET carquet)
        set(CARQUET_LIB carquet PARENT_SCOPE)
        return()
    endif()

    # Avoid pulling OpenMP into the static archive; NeuG links Carquet into a
    # shared extension and OpenMP runtimes are brittle across macOS toolchains.
    set(CMAKE_DISABLE_FIND_PACKAGE_OpenMP TRUE)

    set(CARQUET_BUILD_DEV OFF CACHE BOOL "" FORCE)
    set(CARQUET_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(CARQUET_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(CARQUET_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    set(CARQUET_BUILD_CLI OFF CACHE BOOL "" FORCE)
    set(CARQUET_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    # Disable LTO in the Carquet sub-build to keep static linking predictable.
    set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "" FORCE)

    FetchContent_Declare(
        carquet
        GIT_REPOSITORY https://github.com/Vitruves/carquet.git
        GIT_TAG v0.5.1
        ${FC_DECLARE_COMMON_OPTIONS})
    FetchContent_MakeAvailable(carquet)

    if(NOT TARGET carquet)
        message(FATAL_ERROR "Carquet fetch succeeded but target 'carquet' was not created")
    endif()

    set(CARQUET_LIB carquet PARENT_SCOPE)
    message(STATUS "Carquet library ready: carquet")
endfunction()
