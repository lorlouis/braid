#!/usr/bin/env bash
# Build and install `brd`.
#
# `libghostty-vt-sys` builds Ghostty from a pinned commit whose `build.zig.zon`
# names `minimum_zig_version = "0.16.0"`. Zig's check requires an exact major and
# minor with a patch at or above the floor, so any 0.16.x host toolchain is used
# as-is; otherwise one is fetched into a cache directory and put on PATH for this
# build alone.

set -euo pipefail

ZIG_VERSION="0.16.0"
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PREFIX="${BRD_PREFIX:-$HOME/.local/bin}"
ZIG_CACHE="${BRD_ZIG_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/braid/zig}"
ZIG_SYSTEM_DIR="${GHOSTTY_ZIG_SYSTEM_DIR:-}"
ZIG_PACKAGE_CACHE="${ZIG_GLOBAL_CACHE_DIR:-}"

usage() {
    cat <<EOF
usage: install.sh [--prefix DIR] [--zig-dir DIR] [--zig-system-dir DIR]

  --prefix DIR          where to install brd    (default: $HOME/.local/bin)
  --zig-dir DIR         cache for a fetched Zig (default: \${XDG_CACHE_HOME:-\$HOME/.cache}/braid/zig)
  --zig-system-dir DIR  resolve Ghostty's Zig packages from DIR instead of
                        letting Zig fetch them. Also read from
                        GHOSTTY_ZIG_SYSTEM_DIR. Fill DIR on a host that can
                        reach the package hosts by running
                        Ghostty's \`nix/build-support/fetch-zig-cache.sh\` with
                        ZIG_GLOBAL_CACHE_DIR set; pass its \`p\` directory here.

A host \`zig\` is used when its major and minor match $ZIG_VERSION. Otherwise that
release is downloaded, checksum-verified, and used for this build alone; it is
never installed system-wide.

Ghostty's source is fetched by \`libghostty-vt-sys\`'s build script at the commit
that crate pins, so the build needs network access to github.com.

If Zig cannot fetch HTTPS through a TLS-intercepting proxy but curl can, the
installer automatically fills a private Zig package cache with curl and retries.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --prefix) PREFIX="${2:?--prefix needs a directory}"; shift 2 ;;
        --zig-dir) ZIG_CACHE="${2:?--zig-dir needs a directory}"; shift 2 ;;
        --zig-system-dir) ZIG_SYSTEM_DIR="${2:?--zig-system-dir needs a directory}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'install.sh: unknown argument %s\n\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done

[ -n "$ZIG_PACKAGE_CACHE" ] || ZIG_PACKAGE_CACHE="$ZIG_CACHE/packages-$ZIG_VERSION"
case "$ZIG_PACKAGE_CACHE" in
    /*) ;;
    *) ZIG_PACKAGE_CACHE="$PWD/$ZIG_PACKAGE_CACHE" ;;
esac

die() { printf 'install.sh: %s\n' "$*" >&2; exit 1; }
note() { printf '==> %s\n' "$*"; }
warn() { printf 'install.sh: %s\n' "$*" >&2; }

need() { command -v "$1" >/dev/null 2>&1 || die "$1 is required but not on PATH"; }

# Zig names its releases <arch>-<os>, which is neither uname's spelling nor
# Rust's triple.
zig_platform() {
    local os arch
    case "$(uname -s)" in
        Linux) os="linux" ;;
        Darwin) os="macos" ;;
        *) die "unsupported OS $(uname -s); build manually with Zig $ZIG_VERSION on PATH" ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64) arch="x86_64" ;;
        aarch64|arm64) arch="aarch64" ;;
        *) die "unsupported architecture $(uname -m); build manually with Zig $ZIG_VERSION on PATH" ;;
    esac
    printf '%s-%s' "$arch" "$os"
}

# Pinned rather than fetched from the release index: an install script that
# downloads a compiler without checking what it got is a supply-chain hole.
zig_sha256() {
    case "$1" in
        x86_64-linux)  printf '70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00' ;;
        aarch64-linux) printf 'ea4b09bfb22ec6f6c6ceac57ab63efb6b46e17ab08d21f69f3a48b38e1534f17' ;;
        x86_64-macos)  printf '0387557ed1877bc6a2e1802c8391953baddba76081876301c522f52977b52ba7' ;;
        aarch64-macos) printf 'b23d70deaa879b5c2d486ed3316f7eaa53e84acf6fc9cc747de152450d401489' ;;
        *) die "no pinned checksum for $1" ;;
    esac
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

download() {
    local url="$1" out="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --proto '=https' --tlsv1.2 -o "$out" "$url"
    else
        wget -q --https-only -O "$out" "$url"
    fi
}

# Ghostty's `requireZig` compares major and minor exactly and accepts any patch
# at or above the floor, so a host 0.16.1 is fine and 0.15.x is not.
zig_ok() {
    [ -x "$1" ] || return 1
    local reported major minor patch
    reported="$("$1" version 2>/dev/null)" || return 1
    # A dev build reports 0.16.0-dev.164+bc7955306; only the release part is compared.
    IFS='.-' read -r major minor patch _ <<<"$reported"
    [ "$major.$minor" = "${ZIG_VERSION%.*}" ] || return 1
    case "$patch" in ''|*[!0-9]*) return 1 ;; esac
    [ "$patch" -ge "${ZIG_VERSION##*.}" ]
}

# Sets ZIG_DIR to the directory holding a usable zig, fetching one if needed.
# The directory matters, not the binary: zig resolves its own `lib/` relative to
# argv[0], so the tree cannot be split up.
#
# It assigns rather than printing because bash disables errexit inside command
# substitution: as `$(ensure_zig)` a failed download or extraction would be
# swallowed and misreported as a bad Zig two lines later.
ensure_zig() {
    local host_zig platform dir archive staging got want
    host_zig="$(command -v zig 2>/dev/null || true)"
    if [ -n "$host_zig" ] && zig_ok "$host_zig"; then
        note "using host Zig $("$host_zig" version) ($host_zig)"
        ZIG_DIR="$(dirname -- "$host_zig")"
        return
    fi
    if [ -n "$host_zig" ]; then
        note "host Zig is $("$host_zig" version 2>/dev/null || echo unknown); Ghostty needs ${ZIG_VERSION%.*}.x"
    fi

    platform="$(zig_platform)"
    dir="$ZIG_CACHE/zig-$platform-$ZIG_VERSION"
    if zig_ok "$dir/zig"; then
        note "using cached Zig $("$dir/zig" version) ($dir/zig)"
        ZIG_DIR="$dir"
        return
    fi

    need tar
    # tar delegates .xz to a separate binary, and its failure message names
    # neither xz nor the archive.
    need xz
    command -v curl >/dev/null 2>&1 || need wget
    command -v sha256sum >/dev/null 2>&1 || need shasum

    # Cleanup is explicit rather than a RETURN trap: without `set -T` such a
    # trap leaks past this function and fires on every later return, reading an
    # out-of-scope variable under `set -u`.
    archive="$(mktemp -t "zig-$ZIG_VERSION-XXXXXX.tar.xz")"
    note "downloading Zig $ZIG_VERSION for $platform"
    if ! download "https://ziglang.org/download/$ZIG_VERSION/zig-$platform-$ZIG_VERSION.tar.xz" "$archive"; then
        rm -f "$archive"
        die "cannot download Zig $ZIG_VERSION for $platform"
    fi

    want="$(zig_sha256 "$platform")"
    got="$(sha256_of "$archive")"
    if [ "$got" != "$want" ]; then
        rm -f "$archive"
        die "Zig checksum mismatch: expected $want, got $got"
    fi

    mkdir -p "$ZIG_CACHE"
    rm -rf "$dir"
    # Extract beside the target and rename, so an interrupted run never leaves a
    # partial tree that the next one would trust.
    staging="$(mktemp -d "$ZIG_CACHE/.staging-XXXXXX")"
    if ! tar -xJf "$archive" -C "$staging"; then
        rm -f "$archive"
        rm -rf "$staging"
        die "cannot extract Zig archive"
    fi
    rm -f "$archive"
    mv "$staging/zig-$platform-$ZIG_VERSION" "$dir"
    rmdir "$staging"

    zig_ok "$dir/zig" || die "extracted Zig does not report $ZIG_VERSION"
    note "Zig $("$dir/zig" version) ready at $dir/zig"
    ZIG_DIR="$dir"
}

# `zig fetch` requires a build.zig in the current directory even when its input
# is an explicit URL or local path.
zig_scratch_project() {
    printf 'const std = @import("std");\npub fn build(b: *std.Build) void {\n    _ = b;\n}\n' \
        > "$1/build.zig"
}

# The failed Cargo build leaves the exact pinned Ghostty checkout in OUT_DIR.
# Pick the checkout from the most recently invoked build script without relying
# on GNU find extensions (the installer also runs on macOS).
ghostty_source() {
    local candidate stamp newest="" newest_stamp=""
    if [ -n "${GHOSTTY_SOURCE_DIR:-}" ] \
        && [ -f "$GHOSTTY_SOURCE_DIR/build.zig.zon.txt" ] \
        && [ -f "$GHOSTTY_SOURCE_DIR/build.zig.zon.json" ]
    then
        printf '%s' "$GHOSTTY_SOURCE_DIR"
        return
    fi
    for candidate in "$REPO_ROOT"/target/release/build/libghostty-vt-sys-*/out/ghostty-src; do
        [ -f "$candidate/build.zig.zon.txt" ] || continue
        [ -f "$candidate/build.zig.zon.json" ] || continue
        stamp="${candidate%/out/ghostty-src}/invoked.timestamp"
        if [ -z "$newest" ] \
            || { [ -e "$stamp" ] && { [ ! -e "$newest_stamp" ] || [ "$stamp" -nt "$newest_stamp" ]; }; }
        then
            newest="$candidate"
            newest_stamp="$stamp"
        fi
    done
    [ -n "$newest" ] && printf '%s' "$newest"
}

archive_extension() {
    case "${1%%\?*}" in
        *.tar.xz)  printf '.tar.xz' ;;
        *.tar.zst) printf '.tar.zst' ;;
        *.tgz)     printf '.tgz' ;;
        *)         printf '.tar.gz' ;;
    esac
}

# Hash local package content into Zig's cache. The hash must be one Ghostty
# pinned in its generated transitive lock data; the later build checks the same
# hash when resolving the package.
cache_zig_package() {
    local source="$1" scratch="$2" manifest="$3" got error_file
    [ -n "$scratch" ] && [ -d "$scratch" ] || return 1
    error_file="$scratch/zig-fetch.err"
    if ! got="$(cd "$scratch" && zig fetch \
        --global-cache-dir "$ZIG_PACKAGE_CACHE" "$source" 2>"$error_file")"
    then
        warn "zig could not cache $source: $(<"$error_file")"
        return 1
    fi
    if [ -z "$got" ] || ! grep -Fq "\"$got\":" "$manifest"; then
        warn "downloaded package produced an unpinned Zig hash: ${got:-<none>}"
        return 1
    fi
    printf '    %s\n' "$got"
}

prefetch_zig_archive() {
    local url="$1" scratch="$2" manifest="$3" extension archive
    [ -n "$scratch" ] && [ -d "$scratch" ] || return 1
    extension="$(archive_extension "$url")"
    archive="$scratch/package$extension"
    rm -f "$scratch"/package.*
    if ! download "$url" "$archive"; then
        warn "failed to download $url"
        return 1
    fi
    cache_zig_package "$archive" "$scratch" "$manifest"
}

prefetch_zig_git() {
    local spec="$1" scratch="$2" manifest="$3" repo commit checkout resolved
    [ -n "$scratch" ] && [ -d "$scratch" ] || return 1
    spec="${spec#git+}"
    repo="${spec%%#*}"
    commit="${spec##*#}"
    if [ "$repo" = "$commit" ] || [ -z "$commit" ]; then
        warn "invalid pinned Zig git dependency: $spec"
        return 1
    fi

    checkout="$scratch/git-package"
    rm -rf "$checkout"
    git init --quiet "$checkout"
    if ! git -C "$checkout" fetch --quiet --depth=1 "$repo" "$commit" \
        || ! git -C "$checkout" checkout --quiet --detach FETCH_HEAD
    then
        warn "failed to fetch Zig git dependency $repo at $commit"
        return 1
    fi
    resolved="$(git -C "$checkout" rev-parse HEAD)"
    if [ "$resolved" != "$commit" ]; then
        warn "Zig git dependency resolved to $resolved instead of $commit"
        return 1
    fi
    rm -rf "$checkout/.git"
    cache_zig_package "$checkout" "$scratch" "$manifest"
}

# Ghostty's generated URL list is already the complete transitive closure. This
# avoids both Zig's incomplete transitive fetch mode and recursively parsing ZON.
prefetch_zig_packages() {
    local source="$1" list manifest scratch url
    list="$source/build.zig.zon.txt"
    manifest="$source/build.zig.zon.json"
    if ! scratch="$(mktemp -d "${TMPDIR:-/tmp}/brd-zig-fetch.XXXXXX")" \
        || [ -z "$scratch" ] \
        || [ ! -d "$scratch" ]
    then
        warn "cannot create a temporary directory for Zig packages"
        return 1
    fi
    if ! zig_scratch_project "$scratch" || ! mkdir -p "$ZIG_PACKAGE_CACHE"; then
        warn "cannot prepare the Zig package cache"
        rm -rf "$scratch"
        return 1
    fi

    note "pre-fetching Ghostty's Zig packages with curl"
    while IFS= read -r url || [ -n "$url" ]; do
        [ -n "$url" ] || continue
        case "$url" in
            https://*)
                if ! prefetch_zig_archive "$url" "$scratch" "$manifest"; then
                    rm -rf "$scratch"
                    return 1
                fi
                ;;
            git+https://*)
                if ! prefetch_zig_git "$url" "$scratch" "$manifest"; then
                    rm -rf "$scratch"
                    return 1
                fi
                ;;
            *)
                warn "unsupported Zig dependency URL: $url"
                rm -rf "$scratch"
                return 1
                ;;
        esac
    done < "$list"

    rm -rf "$scratch"
}

# True only when Zig cannot fetch a real Ghostty archive but the system TLS
# stack can download it and Zig can consume that local copy. An isolated cache
# prevents an earlier fetch from hiding a broken network path.
zig_needs_curl() {
    local source="$1" scratch="" seen_hosts="" url host extension archive got
    if ! scratch="$(mktemp -d "${TMPDIR:-/tmp}/brd-zig-probe.XXXXXX")" \
        || [ -z "$scratch" ] \
        || [ ! -d "$scratch" ]
    then
        warn "cannot create a temporary directory for the Zig network probe"
        return 1
    fi
    if ! zig_scratch_project "$scratch"; then
        warn "cannot prepare the Zig network probe"
        rm -rf "$scratch"
        return 1
    fi

    # One real package per host catches a proxy or certificate path that only
    # affects GitHub or deps.files.ghostty.org without downloading every package.
    while IFS= read -r url || [ -n "$url" ]; do
        case "$url" in https://*) ;; *) continue ;; esac
        host="${url#https://}"
        host="${host%%/*}"
        case " $seen_hosts " in *" $host "*) continue ;; esac
        seen_hosts="$seen_hosts $host"

        if (cd "$scratch" && zig fetch \
            --global-cache-dir "$scratch/remote-cache" "$url") >/dev/null 2>&1
        then
            continue
        fi

        extension="$(archive_extension "$url")"
        archive="$scratch/probe$extension"
        rm -f "$scratch"/probe.*
        if ! download "$url" "$archive"; then
            warn "Zig and curl both failed to download from $host"
            continue
        fi
        if got="$(cd "$scratch" && zig fetch \
            --global-cache-dir "$scratch/local-cache" "$archive" 2>/dev/null)" \
            && [ -n "$got" ] \
            && grep -Fq "\"$got\":" "$source/build.zig.zon.json"
        then
            rm -rf "$scratch"
            return 0
        fi
        warn "curl downloaded from $host, but Zig could not verify the local package"
    done < "$source/build.zig.zon.txt"

    rm -rf "$scratch"
    return 1
}

# Every `brd` on this PATH, in order. An SSH login resolves `brd --server`
# through its own PATH, so any stale copy is a candidate, and one speaking an
# older protocol fails as "stream ended before a complete frame" with the
# version named nowhere.
report_path_conflicts() {
    local installed="$1" dir candidate known conflict=0
    local -a dirs=() seen=()
    # Scoped to the read: joining `seen` for the duplicate test below needs the
    # default IFS back, and PATH routinely lists the same directory twice.
    IFS=: read -r -a dirs <<< "$PATH"
    for dir in "${dirs[@]}"; do
        [ -n "$dir" ] || dir="."
        candidate="$dir/brd"
        if [ ! -f "$candidate" ] || [ ! -x "$candidate" ]; then
            continue
        fi
        for known in ${seen[@]+"${seen[@]}"}; do
            [ "$known" = "$candidate" ] && continue 2
        done
        seen+=("$candidate")
        cmp -s "$candidate" "$installed" || conflict=1
    done

    [ "${#seen[@]}" -gt 1 ] || return 0

    if [ "$conflict" -eq 0 ]; then
        note "${#seen[@]} copies of brd on PATH, all this build:"
        printf '    %s\n' "${seen[@]}"
        return 0
    fi

    printf '\n'
    warn "more than one brd is on PATH:"
    for candidate in "${seen[@]}"; do
        if cmp -s "$candidate" "$installed"; then
            printf '    %s  (this build)\n' "$candidate" >&2
        else
            printf '    %s  (differs)\n' "$candidate" >&2
        fi
    done
    warn "a login shell picks one by its own PATH order, which need not match this one."
    warn "install over the stale copies too, or remove them:"
    for candidate in "${seen[@]}"; do
        cmp -s "$candidate" "$installed" || printf '        install -m 0755 %s %s\n' "$installed" "$candidate" >&2
    done
}

need cargo
need git

ensure_zig
export PATH="$ZIG_DIR:$PATH"

if [ -n "$ZIG_SYSTEM_DIR" ]; then
    [ -d "$ZIG_SYSTEM_DIR" ] || die "no such directory: $ZIG_SYSTEM_DIR"
    export GHOSTTY_ZIG_SYSTEM_DIR="$ZIG_SYSTEM_DIR"
    note "resolving Zig packages from $ZIG_SYSTEM_DIR"
else
    mkdir -p "$ZIG_PACKAGE_CACHE"
    export ZIG_GLOBAL_CACHE_DIR="$ZIG_PACKAGE_CACHE"
fi

build_failed() {
    printf '\n' >&2
    warn "the release build failed."
    warn "automatic curl fallback only handles Zig package download failures."
    warn "to prepare a complete package directory on another host:"
    printf '\n        cd /path/to/ghostty\n' >&2
    printf '        ZIG_GLOBAL_CACHE_DIR=/tmp/ghostty-zig ./nix/build-support/fetch-zig-cache.sh\n\n' >&2
    warn "copy that directory to this host and build against it:"
    printf '\n        %s --zig-system-dir /path/to/ghostty-zig/p\n\n' "$0" >&2
    exit 1
}

note "building brd (the first Zig build takes a few minutes)"
if ! cargo build --release --locked --manifest-path "$REPO_ROOT/Cargo.toml"; then
    [ -z "$ZIG_SYSTEM_DIR" ] || build_failed

    ghostty_dir="$(ghostty_source || true)"
    if [ -z "$ghostty_dir" ]; then
        warn "Cargo failed before materializing Ghostty's package metadata"
        build_failed
    fi
    if ! command -v curl >/dev/null 2>&1; then
        warn "curl is required for the Zig TLS fallback"
        build_failed
    fi
    if ! zig_needs_curl "$ghostty_dir"; then
        warn "Zig can fetch Ghostty packages, or curl cannot provide a usable replacement"
        build_failed
    fi

    note "Zig cannot fetch Ghostty packages directly; curl can"
    prefetch_zig_packages "$ghostty_dir" || build_failed
    note "retrying the release build with the local Zig package cache"
    cargo build --release --locked --manifest-path "$REPO_ROOT/Cargo.toml" || build_failed
fi

mkdir -p "$PREFIX"
install -m 0755 "$REPO_ROOT/target/release/brd" "$PREFIX/brd"
note "installed $PREFIX/brd"

# The daemon holds live PTYs, so it is never killed here. The socket name is
# stable across versions, so this build talks to the daemon already holding the
# user's shells and the two settle their protocol version in the handshake.
report_path_conflicts "$PREFIX/brd"

case ":$PATH:" in
    *":$PREFIX:"*) ;;
    *)
        # A login shell that resolves no `brd` at all falls back to
        # $HOME/.local/bin - see SERVER_COMMAND in
        # crates/client/src/transport.rs - so only another prefix is out of
        # reach of a client.
        [ "$PREFIX" = "$HOME/.local/bin" ] \
            || warn "$PREFIX is not on PATH; brd will not be found by an SSH login"
        ;;
esac

# systemd kills a user's whole slice at last logout unless lingering is on. The
# daemon leaves sshd's process group, so it survives the connection that
# started it - but not that, and a session it is still running is then reachable
# only by pid.
report_linger() {
    local user
    command -v loginctl >/dev/null 2>&1 || return 0
    user="$(id -un)"
    [ "$(loginctl show-user "$user" --property=Linger --value 2>/dev/null)" = "yes" ] && return 0

    printf '\n'
    warn "systemd will end your user slice at last logout, taking every detached brd session with it."
    warn "to keep sessions across logouts:"
    printf '        sudo loginctl enable-linger %s\n' "$user" >&2
}

report_linger
