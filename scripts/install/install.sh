#!/usr/bin/env bash
# 构建并安装 bbtools-infra 到用户前缀（默认 ~/.local）。
# 依赖通过显式源码路径接入，避免使用旧的 /usr/local 库。
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
coroutine_dir="${BBT_COROUTINE_SOURCE_DIR:-${repo_root}/../bbtools-coroutine}"
prefix="${BBT_INSTALL_PREFIX:-${HOME}/.local}"
build_dir="${BBT_BUILD_DIR:-${repo_root}/build-install}"
jobs="${BBT_JOBS:-2}"

if [ ! -f "${coroutine_dir}/CMakeLists.txt" ]; then
    echo "找不到 bbtools-coroutine：${coroutine_dir}" >&2
    exit 1
fi

cmake -S "${repo_root}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBBT_COROUTINE_SOURCE_DIR="${coroutine_dir}" \
    -DNEED_TEST=OFF -DBUILD_TESTING=OFF
cmake --build "${build_dir}" --parallel "${jobs}"

install -d "${prefix}/include" "${prefix}/lib"

copy_headers() {
    local rel="$1"
    local strip_root="$2"
    while IFS= read -r -d '' file; do
        local target="${prefix}/include/${file#"${repo_root}/${strip_root}/"}"
        install -D -m 0644 "${file}" "${target}"
    done < <(find "${repo_root}/${rel}" -type f \( -name '*.h' -o -name '*.hh' -o -name '*.hpp' \) -print0)
}

copy_headers include include
copy_headers bbt/infra .

while IFS= read -r -d '' file; do
    install -m 0644 "${file}" "${prefix}/lib/$(basename "${file}")"
done < <(find "${build_dir}" -type f \( -name 'libbbt_infra*.a' -o -name 'libbbt_infra*.so' -o -name 'libbbt_infra*.so.*' \) -print0)

printf '安装完成：%s\n' "${prefix}"
