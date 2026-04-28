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

if(WIN32)
    set(SEEKDB_PORT_BACKEND src/port_win32.c)
else()
    set(SEEKDB_PORT_BACKEND src/port_posix.c)
endif()

add_library(seekdb_client
            src/seekdb.cc
            ${SEEKDB_PORT_BACKEND})

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

# test_one_client_process and test_two_clients_processes use POSIX
# fork()/pipe()/waitpid() and don't have a Win32 equivalent — skip them
# under WIN32. test_two_clients_threads is cross-platform.
if(NOT WIN32)
    add_executable(test_one_client_process tests/test_one_client_process.cc)
    target_include_directories(test_one_client_process PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(test_one_client_process PRIVATE seekdb_client GTest::gtest_main)
    gtest_discover_tests(test_one_client_process DISCOVERY_MODE PRE_TEST)

    add_executable(test_two_clients_processes tests/test_two_clients_processes.cc)
    target_include_directories(test_two_clients_processes PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(test_two_clients_processes PRIVATE seekdb_client GTest::gtest_main)
    gtest_discover_tests(test_two_clients_processes DISCOVERY_MODE PRE_TEST)
endif()

add_executable(test_two_clients_threads tests/test_two_clients_threads.cc)
target_include_directories(test_two_clients_threads PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(test_two_clients_threads PRIVATE seekdb_client GTest::gtest_main)
gtest_discover_tests(test_two_clients_threads DISCOVERY_MODE PRE_TEST)

