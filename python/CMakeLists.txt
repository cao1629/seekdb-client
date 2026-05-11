cmake_minimum_required(VERSION 3.18)
project(python-client LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ------ Find Python + pybind11 ------
find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE _pybind11_rc
)
if(NOT _pybind11_rc EQUAL 0)
    message(FATAL_ERROR "pybind11 not importable in ${Python3_EXECUTABLE}. Install with: pip install --user pybind11")
endif()
message(STATUS "pybind11 cmake dir: ${pybind11_DIR}")
find_package(pybind11 CONFIG REQUIRED)

# ------ seekdb-client locations (override via -D on cmake cmd line) ------
set(SEEKDB_CLIENT_DIR     "$ENV{HOME}/repo/seekdb-client"  CACHE PATH "seekdb-client checkout")
set(SEEKDB_CLIENT_LIB_DIR "${SEEKDB_CLIENT_DIR}/build"     CACHE PATH "Where libseekdb_client.so lives")

# ------ Build the extension ------
# Module name `seekdb_pyclient` — distinct from `libseekdb_client.so` to avoid
# CMake target/link-library name collision.
pybind11_add_module(seekdb_pyclient src/bindings.cpp)

target_include_directories(seekdb_pyclient PRIVATE "${SEEKDB_CLIENT_DIR}/include")
target_link_libraries(seekdb_pyclient PRIVATE
    "${SEEKDB_CLIENT_LIB_DIR}/libseekdb_client.so"
)

# RPATH so libseekdb_client.so resolves at runtime without LD_LIBRARY_PATH
set_target_properties(seekdb_pyclient PROPERTIES
    INSTALL_RPATH         "${SEEKDB_CLIENT_LIB_DIR}"
    BUILD_RPATH           "${SEEKDB_CLIENT_LIB_DIR}"
    BUILD_WITH_INSTALL_RPATH TRUE
)
