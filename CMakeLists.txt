cmake_minimum_required(VERSION 3.14)
project(seekdb_client C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

enable_testing()

# On Windows, <windows.h> by default pulls in the legacy <winsock.h>,
# which clashes with <winsock2.h> used by mariadb-connector-c's SCHANNEL
# backend (sockaddr_in, fd_set, timeval, etc. all redefined). Defining
# WIN32_LEAN_AND_MEAN before <windows.h> excludes the legacy winsock.
if (WIN32)
    add_compile_definitions(WIN32_LEAN_AND_MEAN)
endif()

# When cross-compiling for Android, the NDK toolchain sets CMAKE_FIND_ROOT_PATH
# to the cross sysroot only. find_package(OpenSSL) will ignore OPENSSL_ROOT_DIR
# if it's outside that path. Whitelist our project-built OpenSSL so mariadb-
# connector-c picks it up.
if (ANDROID AND DEFINED OPENSSL_ROOT_DIR)
    list(APPEND CMAKE_FIND_ROOT_PATH "${OPENSSL_ROOT_DIR}")
endif()

# ----------------------------------------------------------------------------
# libmariadb — vendored as a git submodule at deps/mariadb-connector-c.
# ----------------------------------------------------------------------------

if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/deps/mariadb-connector-c/CMakeLists.txt")
    message(FATAL_ERROR
        "deps/mariadb-connector-c is empty.  Run:\n"
        "    git submodule update --init --recursive")
endif()

# Trim mariadb-connector-c down to what we need.
#   WITH_SSL is mandatory upstream (must be OPENSSL/GNUTLS/SCHANNEL), so
#   we leave it ON — the submodule will use OpenSSL via the system on Linux
#   and via Homebrew on macOS.
set(WITH_UNIT_TESTS    OFF CACHE BOOL "" FORCE)
set(WITH_CURL          OFF CACHE BOOL "" FORCE)
set(WITH_DOCS          OFF CACHE BOOL "" FORCE)

# mariadb-connector-c on Windows references ${ZLIB_LIBRARY} in libmariadb's
# link list even when the build is using its own bundled zlib (built as a
# static target named `zlib` under deps/mariadb-connector-c/external/zlib).
# Without this, ZLIB_LIBRARY is empty/NOTFOUND and CMake fails generation.
# On macOS/Linux find_package(ZLIB) finds the system zlib, so this is a
# Windows-only workaround.
if(WIN32)
    set(ZLIB_LIBRARY zlib CACHE STRING "" FORCE)
endif()

# seekdb_client is built static today, but if downstream consumers ever wrap
# it in a shared library we need PIC code for the bundled libmariadb.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# mariadb-connector-c's install rules reference INSTALL_PLUGINDIR /
# INSTALL_LIBDIR even under EXCLUDE_FROM_ALL. Set them explicitly so
# the install commands have a destination (we never actually install).
include(GNUInstallDirs)
set(INSTALL_PLUGINDIR "lib/mariadb/plugin")
set(INSTALL_LIBDIR    "lib")

add_subdirectory(deps/mariadb-connector-c EXCLUDE_FROM_ALL)

# Headers come from two places: the source tree (the bulk of the C API)
# and the build tree (configure-generated mariadb_version.h, ma_config.h).
set(MARIADB_INCLUDE_DIRS
    "${CMAKE_CURRENT_SOURCE_DIR}/deps/mariadb-connector-c/include"
    "${CMAKE_CURRENT_BINARY_DIR}/deps/mariadb-connector-c/include")
set(MARIADB_LIBRARY mariadbclient)   # CMake target from the submodule

# ----------------------------------------------------------------------------
# seekdb_client library
# ----------------------------------------------------------------------------

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_library(seekdb_client SHARED
            src/seekdb.cc
            src/port.cc)

target_include_directories(seekdb_client
    PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${MARIADB_INCLUDE_DIRS})

target_link_libraries(seekdb_client PRIVATE ${MARIADB_LIBRARY} Threads::Threads)

if (NOT MSVC)
    target_compile_options(seekdb_client PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()

# ----------------------------------------------------------------------------
# CLI tool
# ----------------------------------------------------------------------------

add_executable(seekdb_cli tools/seekdb_cli.c)
target_include_directories(seekdb_cli PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${MARIADB_INCLUDE_DIRS})
target_link_libraries(seekdb_cli PRIVATE seekdb_client)

# ----------------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------------

# Google Test — vendored as a git submodule at deps/googletest, pinned
# to v1.14.0. Run `git submodule update --init --recursive` after a
# fresh clone.
if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/deps/googletest/CMakeLists.txt")
    message(FATAL_ERROR
        "deps/googletest is empty.  Run:\n"
        "    git submodule update --init --recursive")
endif()

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
add_subdirectory(deps/googletest EXCLUDE_FROM_ALL)
include(GoogleTest)

# Per-test data dirs live under SEEKDB_TEST_DATA_ROOT, given to each
# test as a compile-time string. Tests append their own subdir name so
# different fixtures don't trample each other.
#
# On POSIX we use /tmp/seekdb_tests because the build tree path is
# typically too long for AF_UNIX's sun_path[108] limit (the Unix socket
# at <db_dir>/run/sql.sock blows past 108 chars otherwise). Windows
# uses named pipes which have no such limit, so the build tree is fine.
if(WIN32)
    set(SEEKDB_TEST_DATA_ROOT "${CMAKE_CURRENT_BINARY_DIR}/test_data")
else()
    set(SEEKDB_TEST_DATA_ROOT "/tmp/seekdb_tests")
endif()

add_executable(test_one_client_process tests/test_one_client_process.cc)
target_include_directories(test_one_client_process PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_definitions(test_one_client_process PRIVATE SEEKDB_TEST_DATA_ROOT="${SEEKDB_TEST_DATA_ROOT}")
target_link_libraries(test_one_client_process PRIVATE seekdb_client GTest::gtest_main)
gtest_discover_tests(test_one_client_process DISCOVERY_MODE PRE_TEST)

add_executable(test_two_clients_processes tests/test_two_clients_processes.cc)
target_include_directories(test_two_clients_processes PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_definitions(test_two_clients_processes PRIVATE SEEKDB_TEST_DATA_ROOT="${SEEKDB_TEST_DATA_ROOT}")
target_link_libraries(test_two_clients_processes PRIVATE seekdb_client GTest::gtest_main)
gtest_discover_tests(test_two_clients_processes DISCOVERY_MODE PRE_TEST)

add_executable(test_two_clients_threads tests/test_two_clients_threads.cc)
target_include_directories(test_two_clients_threads PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src ${MARIADB_INCLUDE_DIRS})
target_compile_definitions(test_two_clients_threads PRIVATE SEEKDB_TEST_DATA_ROOT="${SEEKDB_TEST_DATA_ROOT}")
target_link_libraries(test_two_clients_threads PRIVATE seekdb_client GTest::gtest_main)
gtest_discover_tests(test_two_clients_threads DISCOVERY_MODE PRE_TEST)

