#!/usr/bin/env bash
# prepare-deps job 的依赖准备编排：hosted 复现锁定 Boost 1.90 + 经本仓唯一
# recipe 源码构建 hiredis/mongoc/mongocxx 私有前缀 + 按 framework 已发布、
# 固定完整 40 位 SHA（sha256 fail-closed 校验后）执行已发布配方复现锁定
# protobuf 3.21.12 私有前缀；再校验 coroutine 实际 SHA 与 pin 命中，
# 记录上游版本供 workflow output。
#
# 用法：
#   prepare_deps.sh --repo-dir <infra checkout> --coroutine-dir <dir> \
#       --core-dir <dir> --boost-prefix <dir> --deps-prefix <dir> \
#       --protobuf-prefix <dir> --protobuf-recipe-url <url> \
#       --protobuf-recipe-sha256 <64hex> --coroutine-pin-sha <40hex> --jobs N
set -euo pipefail

REPO_DIR=""
COROUTINE_DIR=""
CORE_DIR=""
BOOST_PREFIX=""
DEPS_PREFIX=""
PROTOBUF_PREFIX=""
PROTOBUF_RECIPE_URL=""
PROTOBUF_RECIPE_SHA256=""
COROUTINE_PIN_SHA=""
JOBS="3"

while [ $# -gt 0 ]; do
    case "$1" in
        --repo-dir) REPO_DIR="$2"; shift 2 ;;
        --coroutine-dir) COROUTINE_DIR="$2"; shift 2 ;;
        --core-dir) CORE_DIR="$2"; shift 2 ;;
        --boost-prefix) BOOST_PREFIX="$2"; shift 2 ;;
        --deps-prefix) DEPS_PREFIX="$2"; shift 2 ;;
        --protobuf-prefix) PROTOBUF_PREFIX="$2"; shift 2 ;;
        --protobuf-recipe-url) PROTOBUF_RECIPE_URL="$2"; shift 2 ;;
        --protobuf-recipe-sha256) PROTOBUF_RECIPE_SHA256="$2"; shift 2 ;;
        --coroutine-pin-sha) COROUTINE_PIN_SHA="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        *) echo "[prepare-deps] FATAL: 未知参数 $1" >&2; exit 2 ;;
    esac
done

for var in REPO_DIR COROUTINE_DIR CORE_DIR BOOST_PREFIX DEPS_PREFIX \
           PROTOBUF_PREFIX PROTOBUF_RECIPE_URL PROTOBUF_RECIPE_SHA256 \
           COROUTINE_PIN_SHA JOBS; do
    [ -n "${!var}" ] || { echo "[prepare-deps] FATAL: 缺 --$(echo "$var" | tr 'A-Z_' 'a-z-')" >&2; exit 2; }
done

emit() {  # key=value -> $GITHUB_OUTPUT（离线时 stdout）
    if [ -n "${GITHUB_OUTPUT:-}" ]; then printf '%s\n' "$1" >> "$GITHUB_OUTPUT"; else printf '%s\n' "$1"; fi
}

# T1：checkout 的 coroutine ref 必须恰好是固定 commit；漂移/浅克隆错位一律
# 非零，不静默接受「main 现在恰好是某 SHA」的浮动消费。
coro_ref="$(git -C "$COROUTINE_DIR" rev-parse HEAD)"
emit "coro_ref=${coro_ref}"
test "${coro_ref}" = "${COROUTINE_PIN_SHA}" || {
    echo "FAIL: coroutine HEAD=${coro_ref} != pin ${COROUTINE_PIN_SHA}" >&2
    exit 1
}
git -C "$COROUTINE_DIR" log -1 --format='coroutine pinned HEAD: %h %s (%ci)'

core_ref="$(git -C "$CORE_DIR" rev-parse HEAD)"
emit "core_ref=${core_ref}"
git -C "$CORE_DIR" log -1 --format='core master HEAD: %h %s (%ci)'

# hosted runner 无现役 ARC 镜像的 /opt/boost（Boost 1.90）。按 framework 已发布
# docker/toolchain.lock 的版本/sha256 源码复现锁定 Boost `context` 到工作区私有
# 前缀（下载后 sha256 fail-closed），不改镜像、不装系统包、不写 /usr/local。
bash "${REPO_DIR}/scripts/ci/prepare_boost.sh" "$BOOST_PREFIX" --jobs "$JOBS"

# 复用本仓唯一 recipe，以固定 tag+SHA 源码构建 hiredis/mongoc/mongocxx
# 私有前缀，钉死 decisions/0003+0004 固定版本；前缀落在 workspace 内，经
# deps_bundle.sh 打成 tar 复用。不写 /usr/local、不装系统包。
bash "${REPO_DIR}/scripts/prepare_resource_deps.sh" "$DEPS_PREFIX" --jobs "$JOBS"

# RPC wire 模块所需锁定 protobuf 3.21.12 私有前缀。配方复用 framework
# 已发布、经独立审查的 `scripts/prepare_protobuf.sh`（固定完整 40 位 commit SHA）；
# CI 从该 SHA download 后用由 Git 对象实算的 sha256 fail-closed 校验，再执行，把
# Ubuntu .deb 提取到工作区私有前缀（recipe 内含包 sha256 门禁，amd64 only，不装
# 系统包、不写 /usr/local）。
recipe="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/prepare_protobuf.sh.$$"
curl --fail --location --silent --show-error \
     --max-time 60 --retry 2 --retry-all-errors \
     "$PROTOBUF_RECIPE_URL" -o "$recipe"
printf '%s  %s\n' "$PROTOBUF_RECIPE_SHA256" "$recipe" \
    | sha256sum --check --status \
    || { echo "FAIL: prepare_protobuf.sh sha256 不匹配（来源被篡改/下载损坏），fail-closed" >&2; exit 1; }
echo "复用来源: $PROTOBUF_RECIPE_URL (sha256 ok)"
bash "$recipe" "$PROTOBUF_PREFIX"
