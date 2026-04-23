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

# ----------------------------------------------------------------------------
# Find libmariadb (MariaDB Connector/C)
# ----------------------------------------------------------------------------

set(SEEKDB_LIBMARIADB_DIR "" CACHE PATH "Install prefix of libmariadb")

set(_mariadb_inc_hint "")
set(_mariadb_lib_hint "")
if (SEEKDB_LIBMARIADB_DIR)
    list(APPEND _mariadb_inc_hint "${SEEKDB_LIBMARIADB_DIR}/include")
    list(APPEND _mariadb_lib_hint "${SEEKDB_LIBMARIADB_DIR}/lib")
endif()

find_package(PkgConfig QUIET)
if (PkgConfig_FOUND)
    pkg_check_modules(_PC_MARIADB QUIET libmariadb)
    if (_PC_MARIADB_FOUND)
        list(APPEND _mariadb_inc_hint ${_PC_MARIADB_INCLUDE_DIRS})
        list(APPEND _mariadb_lib_hint ${_PC_MARIADB_LIBRARY_DIRS})
    endif()
endif()

find_path(MARIADB_INCLUDE_DIR
          NAMES mysql.h
          PATH_SUFFIXES mariadb mysql
          HINTS ${_mariadb_inc_hint}
                /opt/homebrew/include /usr/local/include /usr/include)

find_library(MARIADB_LIBRARY
             NAMES mariadb mariadbclient mysqlclient
             HINTS ${_mariadb_lib_hint}
                   /opt/homebrew/lib /usr/local/lib /usr/lib /usr/lib64
                   /usr/lib64/mysql /usr/lib64/mariadb
                   /usr/lib/mysql   /usr/lib/mariadb)

if (NOT MARIADB_INCLUDE_DIR OR NOT MARIADB_LIBRARY)
    message(FATAL_ERROR
        "libmariadb not found.\n"
        "  MARIADB_INCLUDE_DIR=${MARIADB_INCLUDE_DIR}\n"
        "  MARIADB_LIBRARY=${MARIADB_LIBRARY}\n"
        "Install MariaDB Connector/C (e.g. 'brew install mariadb-connector-c'),\n"
        "or pass -DSEEKDB_LIBMARIADB_DIR=/path/to/libmariadb-install-prefix.")
endif()

message(STATUS "libmariadb include: ${MARIADB_INCLUDE_DIR}")
message(STATUS "libmariadb library: ${MARIADB_LIBRARY}")

# ----------------------------------------------------------------------------
# seekdb_client library
# ----------------------------------------------------------------------------

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_library(seekdb_client
            src/seekdb.c)

target_include_directories(seekdb_client
    PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${MARIADB_INCLUDE_DIR})

target_link_libraries(seekdb_client PRIVATE ${MARIADB_LIBRARY} Threads::Threads)

target_compile_options(seekdb_client PRIVATE -Wall -Wextra -Wno-unused-parameter)

# ----------------------------------------------------------------------------
# CLI tool
# ----------------------------------------------------------------------------

add_executable(seekdb_cli tools/seekdb_cli.c)
target_include_directories(seekdb_cli PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${MARIADB_INCLUDE_DIR})
target_link_libraries(seekdb_cli PRIVATE seekdb_client)

# ----------------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------------

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

add_executable(test_two_clients_open tests/test_two_clients_open.cc)
target_link_libraries(test_two_clients_open PRIVATE seekdb_client GTest::gtest_main)
gtest_discover_tests(test_two_clients_open)

add_executable(test_first_open tests/test_first_open.cc)
target_link_libraries(test_first_open PRIVATE seekdb_client GTest::gtest_main)
gtest_discover_tests(test_first_open)

add_executable(test_kill_client tests/test_kill_client.cc)
target_link_libraries(test_kill_client PRIVATE seekdb_client GTest::gtest_main)
gtest_discover_tests(test_kill_client)

find_library(READLINE_LIBRARY readline
    HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
find_path(READLINE_INCLUDE_DIR readline/readline.h
    HINTS /opt/homebrew/include /usr/local/include /usr/include)
if (READLINE_LIBRARY AND READLINE_INCLUDE_DIR)
    target_include_directories(seekdb_cli PRIVATE ${READLINE_INCLUDE_DIR})
    target_link_libraries(seekdb_cli PRIVATE ${READLINE_LIBRARY})
else()
    message(FATAL_ERROR "readline not found")
endif()
