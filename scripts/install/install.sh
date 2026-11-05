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

mongo_args=()
if [ -n "${BBT_MONGOCXX_PREFIX:-}" ] || [ -n "${BBT_MONGOC_PREFIX:-}" ]; then
    mongo_args+=("-DBBT_MONGOCXX_PREFIX=${BBT_MONGOCXX_PREFIX:-}"
                 "-DBBT_MONGOC_PREFIX=${BBT_MONGOC_PREFIX:-}")
fi

cmake -S "${repo_root}" -B "${build_dir}" -G Ninja \
    "${mongo_args[@]}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DBBT_COROUTINE_SOURCE_DIR="${coroutine_dir}" \
    -DNEED_TEST=OFF -DBUILD_TESTING=OFF
cmake --build "${build_dir}" --parallel "${jobs}"

install -d "${prefix}/include" "${prefix}/lib"

# 只安装公开头；detail/debug/schema 中的内部实现不进入消费前缀。
while IFS= read -r -d '' file; do
    install -D -m 0644 "${file}" "${prefix}/include/${file#"${repo_root}/"}"
done < <(find "${repo_root}/bbt/infra" -type f \( -name '*.h' -o -name '*.hh' -o -name '*.hpp' \) \
    ! -path '*/detail/*' ! -path '*/debug/*' ! -path '*/mcp/schema/*' -print0)

while IFS= read -r -d '' file; do
    install -m 0644 "${file}" "${prefix}/lib/$(basename "${file}")"
done < <(find "${build_dir}" -type f \( -name 'libbbt_infra*.a' -o -name 'libbbt_infra*.so' -o -name 'libbbt_infra*.so.*' \) -print0)

printf '安装完成：%s\n' "${prefix}"
