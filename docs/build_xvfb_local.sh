#!/usr/bin/env bash
set -Eeuo pipefail

# Local Xvfb build script for the user's server environment.
# Builds and installs the following into ~/.local:
#   - libXfont2 2.0.7
#   - font-util 1.3.2
#   - xorg-server 21.1.21 (Xvfb only)
#
# Assumptions based on the successful build:
#   - gcc, make, pkg-config, curl, tar, meson, ninja are already available
#   - system xkbcomp exists at /usr/bin/xkbcomp
#   - system XKB data exists at /usr/share/X11/xkb
#
# Usage:
#   bash build_xvfb_local.sh
# or
#   chmod +x build_xvfb_local.sh && ./build_xvfb_local.sh

PREFIX="${PREFIX:-$HOME/.local}"
SRC_ROOT="${SRC_ROOT:-$HOME/src}"
TMPROOT="${TMPROOT:-$HOME/tmp}"
BUILD_JOBS="${BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"

LIBXFONT2_VER="2.0.7"
FONT_UTIL_VER="1.3.2"
XSERVER_VER="21.1.21"

LIBXFONT2_TARBALL="libXfont2-${LIBXFONT2_VER}.tar.xz"
FONT_UTIL_TARBALL="font-util-${FONT_UTIL_VER}.tar.gz"
XSERVER_TARBALL="xorg-server-${XSERVER_VER}.tar.xz"

LIBXFONT2_URL="https://www.x.org/releases/individual/lib/${LIBXFONT2_TARBALL}"
FONT_UTIL_URL="https://www.x.org/releases/individual/font/${FONT_UTIL_TARBALL}"
XSERVER_URL="https://www.x.org/releases/individual/xserver/${XSERVER_TARBALL}"

XKB_BIN_DIR="${XKB_BIN_DIR:-/usr/bin}"
XKB_DIR="${XKB_DIR:-/usr/share/X11/xkb}"
XKB_OUTPUT_DIR="${XKB_OUTPUT_DIR:-$PREFIX/var/lib/xkb}"
FONTROOTDIR="${FONTROOTDIR:-$PREFIX/share/fonts/X11}"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

fail() {
  printf '\n[ERROR] %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

fetch_and_extract() {
  local url="$1"
  local tarball="$2"
  local src_dir="$3"

  mkdir -p "$SRC_ROOT"
  cd "$SRC_ROOT"

  if [[ ! -f "$tarball" ]]; then
    log "Downloading $tarball"
    curl -fL -o "$tarball" "$url"
  else
    log "Using existing tarball: $tarball"
  fi

  if [[ ! -d "$src_dir" ]]; then
    log "Extracting $tarball"
    tar -xf "$tarball"
  else
    log "Using existing source directory: $src_dir"
  fi
}

setup_env() {
  mkdir -p "$TMPROOT" "$PREFIX" "$PREFIX/var/lib" "$XKB_OUTPUT_DIR"

  export TMPDIR="$TMPROOT"
  export TEMP="$TMPROOT"
  export TMP="$TMPROOT"
  export PREFIX
  export PATH="$PREFIX/bin:$PATH"
  export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:$PREFIX/share/pkgconfig:${PKG_CONFIG_PATH:-}"
  export LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:${LD_LIBRARY_PATH:-}"
}

build_libxfont2() {
  local src_dir="libXfont2-${LIBXFONT2_VER}"
  fetch_and_extract "$LIBXFONT2_URL" "$LIBXFONT2_TARBALL" "$src_dir"

  cd "$SRC_ROOT/$src_dir"
  log "Building $src_dir"

  rm -f config.cache
  make distclean >/dev/null 2>&1 || true

  local build_triplet
  build_triplet="$(gcc -dumpmachine)"

  ./configure --prefix="$PREFIX" --build="$build_triplet"
  make -j"$BUILD_JOBS"
  make install

  log "Verifying xfont2 pkg-config"
  pkg-config --modversion xfont2 >/dev/null
}

build_fontutil() {
  local src_dir="font-util-${FONT_UTIL_VER}"
  fetch_and_extract "$FONT_UTIL_URL" "$FONT_UTIL_TARBALL" "$src_dir"

  cd "$SRC_ROOT/$src_dir"
  log "Building $src_dir"

  rm -f config.cache
  make distclean >/dev/null 2>&1 || true

  local build_triplet
  build_triplet="$(gcc -dumpmachine)"

  ./configure --prefix="$PREFIX" --build="$build_triplet" --with-fontrootdir="$FONTROOTDIR"
  make -j"$BUILD_JOBS"
  make install

  log "Verifying fontutil pkg-config"
  pkg-config --modversion fontutil >/dev/null
}

build_xserver_xvfb() {
  local src_dir="xorg-server-${XSERVER_VER}"
  fetch_and_extract "$XSERVER_URL" "$XSERVER_TARBALL" "$src_dir"

  cd "$SRC_ROOT/$src_dir"
  log "Configuring $src_dir (Xvfb only)"

  rm -rf build

  meson setup build \
    --prefix="$PREFIX" \
    -Dxvfb=true \
    -Dxorg=false \
    -Dxephyr=false \
    -Dxnest=false \
    -Ddocs=false \
    -Ddevel-docs=false \
    -Ddocs-pdf=false \
    -Dxkb_bin_dir="$XKB_BIN_DIR" \
    -Dxkb_dir="$XKB_DIR" \
    -Dxkb_output_dir="$XKB_OUTPUT_DIR"

  log "Compiling Xvfb"
  meson compile -C build

  log "Installing Xvfb"
  meson install -C build
}

verify_install() {
  log "Installed binaries"
  command -v Xvfb || true
  ls -l "$PREFIX/bin/Xvfb" || true

  log "pkg-config checks"
  printf 'xfont2 version: %s\n' "$(pkg-config --modversion xfont2)"
  printf 'fontutil version: %s\n' "$(pkg-config --modversion fontutil)"
  printf 'fontrootdir: %s\n' "$(pkg-config --variable=fontrootdir fontutil)"

  log "Smoke test"
  "$PREFIX/bin/Xvfb" :99 -screen 0 1280x1024x24 >"$TMPROOT/xvfb-smoke.log" 2>&1 &
  local xvfb_pid=$!
  sleep 1
  DISPLAY=:99 xdpyinfo >/dev/null 2>&1 && echo "Xvfb OK" || {
    cat "$TMPROOT/xvfb-smoke.log" >&2 || true
    kill "$xvfb_pid" >/dev/null 2>&1 || true
    fail "Xvfb smoke test failed"
  }
  kill "$xvfb_pid" >/dev/null 2>&1 || true
  wait "$xvfb_pid" 2>/dev/null || true

  cat <<EOM

Build finished successfully.

Add these lines before using Xvfb in future shells:
  export PATH="$PREFIX/bin:\$PATH"
  export LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:\${LD_LIBRARY_PATH:-}"

Example usage:
  Xvfb :99 -screen 0 1280x1024x24 &
  XVFB_PID=\$!
  export DISPLAY=:99
  python your_script.py
  kill \$XVFB_PID

Notes:
  - This build uses the system xkbcomp at: $XKB_BIN_DIR/xkbcomp
  - XKB data directory: $XKB_DIR
  - Local XKB output directory: $XKB_OUTPUT_DIR
  - The warning about /tmp/.X11-unix not being created is expected for non-root users.
EOM
}

main() {
  require_cmd gcc
  require_cmd make
  require_cmd pkg-config
  require_cmd curl
  require_cmd tar
  require_cmd meson
  require_cmd ninja
  require_cmd xdpyinfo

  if [[ ! -x "$XKB_BIN_DIR/xkbcomp" ]]; then
    fail "xkbcomp not found at $XKB_BIN_DIR/xkbcomp"
  fi
  if [[ ! -d "$XKB_DIR" ]]; then
    fail "XKB directory not found: $XKB_DIR"
  fi

  setup_env

  log "PREFIX=$PREFIX"
  log "SRC_ROOT=$SRC_ROOT"
  log "TMPROOT=$TMPROOT"
  log "BUILD_JOBS=$BUILD_JOBS"

  build_libxfont2
  build_fontutil
  build_xserver_xvfb
  verify_install
}

main "$@"
