# Cross-compile to 64-bit Android (arm64-v8a) using the Android NDK.
#
# Usage (from project root):
#   export ANDROID_NDK_HOME=$HOME/Library/Android/sdk/ndk/<version>
#   cmake -S . -B build-android \
#         -DCMAKE_TOOLCHAIN_FILE=cmake/android-arm64-toolchain.cmake
#   cmake --build build-android
#
# Or pass the path explicitly:
#   cmake -S . -B build-android \
#         -DCMAKE_TOOLCHAIN_FILE=cmake/android-arm64-toolchain.cmake \
#         -DANDROID_NDK=<path>

if(NOT DEFINED ANDROID_NDK AND DEFINED ENV{ANDROID_NDK_HOME})
    set(ANDROID_NDK "$ENV{ANDROID_NDK_HOME}")
endif()

if(NOT DEFINED ANDROID_NDK OR ANDROID_NDK STREQUAL "")
    message(FATAL_ERROR
        "ANDROID_NDK_HOME is not set.\n"
        "  Set the environment variable, e.g.:\n"
        "    export ANDROID_NDK_HOME=$HOME/Library/Android/sdk/ndk/<version>\n"
        "  Or pass it on the command line: -DANDROID_NDK=<path>")
endif()

if(NOT EXISTS "${ANDROID_NDK}/build/cmake/android.toolchain.cmake")
    message(FATAL_ERROR
        "Android NDK not found at: ${ANDROID_NDK}\n"
        "  build/cmake/android.toolchain.cmake is missing — check the path.")
endif()

set(ANDROID_ABI      arm64-v8a)
set(ANDROID_PLATFORM android-29)

include(${ANDROID_NDK}/build/cmake/android.toolchain.cmake)
