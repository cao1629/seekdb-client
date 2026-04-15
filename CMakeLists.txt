cmake_minimum_required(VERSION 3.13)
project(seekdb_client C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ----------------------------------------------------------------------------
# Find libmariadb (MariaDB Connector/C)
#
# Search order:
#   1. cmake -DSEEKDB_LIBMARIADB_DIR=/path/to/libmariadb-install-prefix
#   2. pkg-config (libmariadb)
#   3. Homebrew default (/opt/homebrew or /usr/local)
# ----------------------------------------------------------------------------

set(SEEKDB_LIBMARIADB_DIR "" CACHE PATH "Install prefix of libmariadb")

set(_mariadb_inc_hint "")
set(_mariadb_lib_hint "")
if (SEEKDB_LIBMARIADB_DIR)
    list(APPEND _mariadb_inc_hint "${SEEKDB_LIBMARIADB_DIR}/include")
    list(APPEND _mariadb_lib_hint "${SEEKDB_LIBMARIADB_DIR}/lib")
endif()

# pkg-config probe (optional)
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
                   /opt/homebrew/lib /usr/local/lib /usr/lib /usr/lib64)

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

add_library(seekdb_client
            src/seekdb.c)

target_include_directories(seekdb_client
    PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${MARIADB_INCLUDE_DIR})

target_link_libraries(seekdb_client PRIVATE ${MARIADB_LIBRARY})

target_compile_options(seekdb_client PRIVATE -Wall -Wextra -Wno-unused-parameter)

# ----------------------------------------------------------------------------
# Example
# ----------------------------------------------------------------------------

option(SEEKDB_BUILD_EXAMPLES "Build seekdb-client examples" ON)
if (SEEKDB_BUILD_EXAMPLES)
    add_executable(seekdb_hello examples/hello.c)
    target_link_libraries(seekdb_hello PRIVATE seekdb_client)
endif()
