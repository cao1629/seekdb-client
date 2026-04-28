#!/usr/bin/env bash
#
# Cross-compile OpenSSL for Android arm64-v8a from the vendored
# deps/openssl submodule. Output goes to build/openssl-android-build/install/,
# which the `android` preset in CMakePresets.json reads as OPENSSL_ROOT_DIR.
#
# Run once after `git submodule update --init`. Re-run only when the
# OpenSSL submodule pin changes.
#
# Requires:
#   ANDROID_NDK_HOME — pointing at an installed Android NDK.

set -euo pipefail

cd "$(dirname "$0")/.."   # project root

if [ -z "${ANDROID_NDK_HOME:-}" ]; then
    echo "error: ANDROID_NDK_HOME is not set" >&2
    echo "  example:" >&2
    echo "    export ANDROID_NDK_HOME=\$HOME/Library/Android/sdk/ndk/<version>" >&2
    exit 1
fi

OPENSSL_SRC="$PWD/deps/openssl"
if [ ! -f "$OPENSSL_SRC/Configure" ]; then
    echo "error: deps/openssl is empty" >&2
    echo "  run: git submodule update --init deps/openssl" >&2
    exit 1
fi

case "$(uname -s)" in
    Darwin) HOST=darwin-x86_64 ;;
    Linux)  HOST=linux-x86_64 ;;
    *) echo "error: unsupported host: $(uname -s)" >&2; exit 1 ;;
esac

BUILD_DIR="$PWD/build/openssl-android-build"
INSTALL_DIR="$BUILD_DIR/install"

# Export so both Configure and the subsequent `make` invocations see them.
export ANDROID_NDK_ROOT="$ANDROID_NDK_HOME"
export PATH="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$HOST/bin:$PATH"

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

"$OPENSSL_SRC/Configure" android-arm64 -D__ANDROID_API__=29 \
                          --prefix="$INSTALL_DIR" no-shared

make -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc)"
make install_sw

echo
echo "OpenSSL for Android installed at: $INSTALL_DIR"
echo "Now run: cmake --preset android"
