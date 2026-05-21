#!/usr/bin/env bash
set -euo pipefail

LLVM_MAJOR_VERSION=21
LLVM_SYS_PREFIX_ENV="LLVM_SYS_211_PREFIX"

run_as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
        return
    fi

    sudo "$@"
}

record_llvm_prefix() {
    local prefix="$1"

    export "${LLVM_SYS_PREFIX_ENV}=${prefix}"
    if [ -n "${GITHUB_ENV:-}" ]; then
        printf '%s=%s\n' "${LLVM_SYS_PREFIX_ENV}" "${prefix}" >> "${GITHUB_ENV}"
    fi
}

install_macos() {
    if ! command -v brew >/dev/null 2>&1; then
        echo "Homebrew is required to install llvm@${LLVM_MAJOR_VERSION} on macOS" >&2
        exit 1
    fi

    if ! brew --prefix "llvm@${LLVM_MAJOR_VERSION}" >/dev/null 2>&1; then
        brew install "llvm@${LLVM_MAJOR_VERSION}"
    fi

    record_llvm_prefix "$(brew --prefix "llvm@${LLVM_MAJOR_VERSION}")"
}

install_dnf() {
    dnf install -y llvm-devel libffi-devel zlib-devel libxml2-devel libzstd-devel
    record_llvm_prefix "/usr"
}

install_apt() {
    run_as_root apt-get update

    if ! apt-cache show "llvm-${LLVM_MAJOR_VERSION}-dev" >/dev/null 2>&1; then
        local llvm_script
        llvm_script="$(mktemp)"
        curl --proto '=https' --tlsv1.2 -fsSL https://apt.llvm.org/llvm.sh -o "${llvm_script}"
        run_as_root bash "${llvm_script}" "${LLVM_MAJOR_VERSION}"
        rm -f "${llvm_script}"
    fi

    run_as_root apt-get install -y \
        "llvm-${LLVM_MAJOR_VERSION}-dev" \
        libffi-dev \
        libxml2-dev \
        libzstd-dev \
        zlib1g-dev
    record_llvm_prefix "/usr/lib/llvm-${LLVM_MAJOR_VERSION}"
}

case "$(uname -s)" in
    Darwin)
        install_macos
        ;;
    Linux)
        if command -v dnf >/dev/null 2>&1; then
            install_dnf
        elif command -v apt-get >/dev/null 2>&1; then
            install_apt
        else
            echo "Unable to install LLVM build dependencies on this Linux distribution" >&2
            exit 1
        fi
        ;;
    *)
        echo "Unsupported platform: $(uname -s)" >&2
        exit 1
        ;;
esac
