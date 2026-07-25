#!/bin/sh
set -e

# Downstream OMP LCM installer
# Usage: curl -fsSL https://raw.githubusercontent.com/tickernelz/oh-my-pi/main/scripts/install.sh | sh
#
# Options:
#   --binary       Install the downstream Linux x64 release binary (default)
#   --source       Clone tickernelz/oh-my-pi, build its workspace binary, and install it
#   --ref <ref>    Release tag for binary mode, or tag/commit/branch with --source
#   -r <ref>       Shorthand for --ref

REPO="tickernelz/oh-my-pi"
INSTALL_DIR="${PI_INSTALL_DIR:-$HOME/.local/bin}"
MIN_BUN_VERSION="1.3.14"
MODE="binary"
REF=""
MODE_SET=""
TMP_DIR=""
TEMP_BINARY=""
VERIFY_DIR=""
INSTALL_TARGET=""
INSTALL_BACKUP=""
INSTALL_VERIFIED=""

restore_previous_binary() {
    [ -z "$INSTALL_TARGET" ] || rm -f "$INSTALL_TARGET"
    if [ -n "$INSTALL_BACKUP" ] && [ -e "$INSTALL_BACKUP" ]; then
        mv "$INSTALL_BACKUP" "$INSTALL_TARGET"
    fi
    INSTALL_TARGET=""
    INSTALL_BACKUP=""
    INSTALL_VERIFIED=""
}

cleanup() {
    if [ -n "$INSTALL_TARGET" ] && [ "$INSTALL_VERIFIED" != "1" ]; then
        restore_previous_binary
    fi
    [ -z "$TEMP_BINARY" ] || rm -f "$TEMP_BINARY"
    [ -z "$TMP_DIR" ] || rm -rf "$TMP_DIR"
    [ -z "$VERIFY_DIR" ] || rm -rf "$VERIFY_DIR"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

while [ $# -gt 0 ]; do
    case "$1" in
        --source|--binary)
            requested_mode=${1#--}
            if [ -n "$MODE_SET" ] && [ "$MODE" != "$requested_mode" ]; then
                echo "Choose exactly one of --source or --binary" >&2
                exit 1
            fi
            MODE="$requested_mode"
            MODE_SET=1
            shift
            ;;
        --ref|-r)
            shift
            if [ $# -eq 0 ] || [ -z "$1" ]; then
                echo "Missing value for --ref" >&2
                exit 1
            fi
            REF="$1"
            shift
            ;;
        --ref=*)
            REF=${1#*=}
            if [ -z "$REF" ]; then
                echo "Missing value for --ref" >&2
                exit 1
            fi
            shift
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

has_bun() {
    command -v bun >/dev/null 2>&1
}

host_arch() {
    case "$(uname -m)" in
        x86_64|amd64) echo "x64" ;;
        arm64|aarch64) echo "arm64" ;;
        *) uname -m ;;
    esac
}

bun_arch() {
    bun -e 'process.stdout.write(process.arch)' 2>/dev/null
}

version_ge() {
    current=$1
    minimum=$2
    current_major=${current%%.*}
    current_rest=${current#*.}
    current_minor=${current_rest%%.*}
    current_patch=${current_rest#*.}
    current_patch=${current_patch%%.*}
    minimum_major=${minimum%%.*}
    minimum_rest=${minimum#*.}
    minimum_minor=${minimum_rest%%.*}
    minimum_patch=${minimum_rest#*.}
    minimum_patch=${minimum_patch%%.*}

    if [ "$current_major" -ne "$minimum_major" ]; then
        [ "$current_major" -gt "$minimum_major" ]
        return
    fi
    if [ "$current_minor" -ne "$minimum_minor" ]; then
        [ "$current_minor" -gt "$minimum_minor" ]
        return
    fi
    [ "$current_patch" -ge "$minimum_patch" ]
}

require_bun_version() {
    version_raw=$(bun --version 2>/dev/null || true)
    version_clean=${version_raw%%-*}
    if [ -z "$version_clean" ] || ! version_ge "$version_clean" "$MIN_BUN_VERSION"; then
        echo "Bun ${MIN_BUN_VERSION} or newer is required. Current version: ${version_clean:-unknown}" >&2
        echo "Upgrade Bun at https://bun.sh/docs/installation" >&2
        exit 1
    fi
}

install_bun() {
    echo "Installing bun..."
    if command -v bash >/dev/null 2>&1; then
        curl -fsSL https://bun.sh/install | bash
    else
        curl -fsSL https://bun.sh/install | sh
    fi
    export BUN_INSTALL="$HOME/.bun"
    export PATH="$BUN_INSTALL/bin:$PATH"
    require_bun_version
}

install_from_source() {
    if ! command -v git >/dev/null 2>&1; then
        echo "git is required for --source" >&2
        exit 1
    fi
    if ! has_bun; then
        install_bun
    fi
    require_bun_version
    bun_runtime_arch=$(bun_arch)
    if [ -n "$bun_runtime_arch" ] && [ "$bun_runtime_arch" != "$(host_arch)" ]; then
        echo "Bun reports architecture '$bun_runtime_arch' but this host is '$(host_arch)'." >&2
        echo "Install a native Bun before building the downstream fork from source." >&2
        exit 1
    fi

    TMP_DIR=$(mktemp -d)
    repo_url="https://github.com/${REPO}.git"
    if [ -n "$REF" ]; then
        if ! git clone --depth 1 --branch "$REF" "$repo_url" "$TMP_DIR" >/dev/null 2>&1; then
            git clone "$repo_url" "$TMP_DIR"
            (cd "$TMP_DIR" && git checkout "$REF")
        fi
    else
        git clone --depth 1 "$repo_url" "$TMP_DIR"
    fi

    if command -v git-lfs >/dev/null 2>&1; then
        (cd "$TMP_DIR" && git lfs pull)
    fi
    package_path="$TMP_DIR/packages/coding-agent"
    if [ ! -d "$package_path" ]; then
        echo "Expected downstream coding-agent package at $package_path" >&2
        exit 1
    fi
    echo "Installing downstream workspace dependencies..."
    (cd "$TMP_DIR" && bun install --frozen-lockfile) || {
        echo "Failed to install the cloned downstream workspace dependencies" >&2
        exit 1
    }
    echo "Building downstream omp from source..."
    (cd "$package_path" && bun run build) || {
        echo "Failed to build the cloned downstream source" >&2
        exit 1
    }
    built_binary="$package_path/dist/omp"
    if [ ! -x "$built_binary" ]; then
        echo "Expected built downstream binary at $built_binary" >&2
        exit 1
    fi
    source_reported=$("$built_binary" --version 2>/dev/null | tr -d '\r' | tail -n 1)
    source_version=${source_reported#omp/}
    if ! printf '%s\n' "$source_version" | grep -Eq '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-lcm\.(0|[1-9][0-9]*)$'; then
        echo "Built source did not report a downstream LCM version: ${source_reported:-no output}" >&2
        exit 1
    fi
    mkdir -p "$INSTALL_DIR"
    TEMP_BINARY=$(mktemp "$INSTALL_DIR/.omp.new.XXXXXX")
    cp "$built_binary" "$TEMP_BINARY"
    chmod +x "$TEMP_BINARY"
    install_verified_binary "$TEMP_BINARY" "$source_version"
    echo ""
    echo "Installed downstream omp from ${REPO}${REF:+ at $REF}"
    echo "Run 'omp' to get started!"
}

require_binary_platform() {
    os_name=$(uname -s)
    arch_name=$(host_arch)
    if [ "$os_name" != "Linux" ] || [ "$arch_name" != "x64" ]; then
        echo "Downstream LCM release binaries currently support Linux x64 (including WSL) only." >&2
        echo "Detected ${os_name}-${arch_name}. Use --source on a supported source-build host." >&2
        exit 1
    fi
    if [ -f /etc/alpine-release ] || { command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; }; then
        echo "The downstream Linux x64 release binary requires glibc; this host uses musl." >&2
        echo "Re-run with --source to build for this host." >&2
        exit 1
    fi
}

require_release_verifier() {
    if ! command -v openssl >/dev/null 2>&1; then
        echo "Authenticated downstream binary installs require OpenSSL 3 with Ed25519 support." >&2
        echo "Install OpenSSL 3, or use --source to build the checked-out downstream source." >&2
        exit 1
    fi
    openssl_version=$(openssl version 2>/dev/null || true)
    case "$openssl_version" in
        OpenSSL\ 3.*) ;;
        *)
            echo "Authenticated downstream binary installs require OpenSSL 3; found ${openssl_version:-unknown}." >&2
            echo "Upgrade OpenSSL, or use --source to build the checked-out downstream source." >&2
            exit 1
            ;;
    esac
}

write_release_public_key() {
    cat > "$1" <<'PUBLIC_KEY'
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEA1PObMDAzy1CcEElh48DM1yf3Ff1UqqmETbpbXP/iVIw=
-----END PUBLIC KEY-----
PUBLIC_KEY
}

read_signed_asset_hash() {
    final_byte=$(tail -c 1 "$1" | od -An -tu1 | tr -d '[:space:]')
    [ "$final_byte" = "10" ] || return 1
    if LC_ALL=C grep -q '[^ -~]' "$1"; then return 1; fi
    awk -v selected="$2" '
        BEGIN { invalid = 0; found = 0 }
        {
            hash = substr($0, 1, 64)
            separator = substr($0, 65, 2)
            name = substr($0, 67)
            if (length(hash) != 64 || hash !~ /^[0-9a-f]+$/ || separator != "  " ||
                name !~ /^[A-Za-z0-9._-]+$/ || seen[name]++) {
                invalid = 1
            }
            if (name == selected) {
                found++
                expected = hash
            }
        }
        END {
            if (invalid || found != 1) exit 1
            print expected
        }
    ' "$1"
}

extract_downstream_tags() {
    grep -oE '"tag_name"[[:space:]]*:[[:space:]]*"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-lcm\.[1-9][0-9]*"' |
        sed -E 's/.*"([^"]+)"/\1/'
}

fetch_release_tag() {
    if [ -n "$REF" ]; then
        echo "Fetching downstream release $REF..." >&2
        release_json=$(curl -fsSL --connect-timeout 10 --max-time 60 "https://api.github.com/repos/${REPO}/releases/tags/${REF}") || {
            echo "Downstream release tag not found: $REF" >&2
            echo "For branch or commit installs, use --source --ref $REF." >&2
            exit 1
        }
        latest=$(printf '%s\n' "$release_json" | extract_downstream_tags | tail -n 1)
        if [ "$latest" != "$REF" ]; then
            echo "Release $REF is not a downstream <upstream>-lcm.<revision> build." >&2
            exit 1
        fi
    else
        echo "Fetching latest downstream LCM release..." >&2
        release_json=$(curl -fsSL --connect-timeout 10 --max-time 60 "https://api.github.com/repos/${REPO}/releases?per_page=100") || {
            echo "Failed to fetch releases from https://github.com/${REPO}" >&2
            exit 1
        }
        latest=$(
            printf '%s\n' "$release_json" | extract_downstream_tags |
                while IFS= read -r tag; do printf '%s %s\n' "${tag#v}" "$tag"; done |
                sort -V | tail -n 1 | cut -d ' ' -f 2
        )
    fi
    if [ -z "$latest" ]; then
        echo "No downstream LCM release tags were found in ${REPO}." >&2
        exit 1
    fi
    printf '%s\n' "$latest"
}

verify_binary_version() {
    candidate=$1
    expected=$2
    reported=$("$candidate" --version 2>/dev/null | tr -d '\r' | tail -n 1) || return 1
    case "$reported" in
        "$expected"|"omp/$expected") return 0 ;;
        *) return 1 ;;
    esac
}

install_verified_binary() {
    candidate=$1
    expected=$2
    if ! verify_binary_version "$candidate" "$expected"; then
        echo "Candidate binary did not report expected downstream version $expected; existing installation was not changed." >&2
        exit 1
    fi

    INSTALL_TARGET="$INSTALL_DIR/omp"
    INSTALL_BACKUP=""
    INSTALL_VERIFIED=""
    if [ -e "$INSTALL_TARGET" ] || [ -L "$INSTALL_TARGET" ]; then
        INSTALL_BACKUP="$INSTALL_TARGET.$(date +%s).$$.bak"
        mv "$INSTALL_TARGET" "$INSTALL_BACKUP"
    fi
    if ! mv "$candidate" "$INSTALL_TARGET"; then
        restore_previous_binary
        echo "Could not atomically install the downstream binary; the previous installation was restored." >&2
        exit 1
    fi
    TEMP_BINARY=""
    if ! verify_binary_version "$INSTALL_TARGET" "$expected"; then
        restore_previous_binary
        echo "Installed binary failed verification; the previous installation was restored." >&2
        exit 1
    fi
    INSTALL_VERIFIED=1
    [ -z "$INSTALL_BACKUP" ] || rm -f "$INSTALL_BACKUP"
    INSTALL_TARGET=""
    INSTALL_BACKUP=""
}

install_binary() {
    require_binary_platform
    require_release_verifier
    latest=$(fetch_release_tag)
    expected=${latest#v}
    binary_name="omp-linux-x64"
    release_url="https://github.com/${REPO}/releases/download/${latest}"
    echo "Using version: $expected"

    VERIFY_DIR=$(mktemp -d)
    checksums_path="$VERIFY_DIR/SHA256SUMS"
    signature_path="$VERIFY_DIR/SHA256SUMS.sig"
    public_key_path="$VERIFY_DIR/downstream-release-ed25519.pem"
    echo "Authenticating release manifest from ${REPO}..."
    curl -fsSL --connect-timeout 10 --max-time 60 --max-filesize 1048576 \
        "$release_url/SHA256SUMS" -o "$checksums_path"
    curl -fsSL --connect-timeout 10 --max-time 60 --max-filesize 64 \
        "$release_url/SHA256SUMS.sig" -o "$signature_path"
    signature_bytes=$(wc -c < "$signature_path")
    if [ "$signature_bytes" -ne 64 ]; then
        echo "Invalid SHA256SUMS.sig length: expected 64 bytes, received $signature_bytes." >&2
        exit 1
    fi
    write_release_public_key "$public_key_path"
    if ! openssl pkeyutl -verify -pubin -inkey "$public_key_path" -rawin \
        -in "$checksums_path" -sigfile "$signature_path" >/dev/null 2>&1; then
        echo "SHA256SUMS.sig is not valid for the pinned downstream release key." >&2
        exit 1
    fi
    expected_hash=$(read_signed_asset_hash "$checksums_path" "$binary_name") || {
        echo "Signed SHA256SUMS has no unique, valid entry for $binary_name." >&2
        exit 1
    }

    mkdir -p "$INSTALL_DIR"
    TEMP_BINARY=$(mktemp "$INSTALL_DIR/.omp.new.XXXXXX")
    echo "Downloading authenticated $binary_name from ${REPO}..."
    curl -fsSL --connect-timeout 10 --speed-limit 1024 --speed-time 30 \
        "$release_url/$binary_name" -o "$TEMP_BINARY"
    actual_hash=$(openssl dgst -sha256 -r "$TEMP_BINARY" | awk '{ print $1 }')
    if [ "$actual_hash" != "$expected_hash" ]; then
        echo "SHA-256 mismatch for $binary_name; existing installation was not changed." >&2
        exit 1
    fi
    rm -rf "$VERIFY_DIR"
    VERIFY_DIR=""
    chmod +x "$TEMP_BINARY"
    install_verified_binary "$TEMP_BINARY" "$expected"

    echo ""
    echo "Installed authenticated downstream omp to $INSTALL_DIR/omp"
    case ":$PATH:" in
        *":$INSTALL_DIR:"*) echo "Run 'omp' to get started!" ;;
        *) echo "Add $INSTALL_DIR to your PATH, then run 'omp'" ;;
    esac
}

case "$MODE" in
    source) install_from_source ;;
    binary) install_binary ;;
    *) echo "Unsupported install mode: $MODE" >&2; exit 1 ;;
esac
