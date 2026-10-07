#!/usr/bin/env bash
#
# bbtools-infra 资源依赖私有前缀准备入口（唯一 recipe）
#
# 在给定的空目录内，从固定 tag 源码构建并安装三份私有前缀：
#   hiredis v1.4.1 / mongo-c-driver 2.5.4 / mongo-cxx-driver r4.6.0
# 并在前缀根目录生成 manifest（resource-deps.manifest.json），供 framework 资源 CI
# 经 -DBBT_HIREDIS_PREFIX / -DBBT_MONGOCXX_PREFIX / -DBBT_MONGOC_PREFIX 消费。
#
# 固定来源、锁定值、更新流程与消费契约见
# docs/dependencies/resource-prefixes.provenance.md（版本真源；本脚本不得写入猜测值）。
#
# 约束（与 AGENTS.md「第三方依赖策略」、decisions 0003/0004 一致）：
#   - 只写入调用方给定的私有前缀；不写 /usr/local，不 sudo，不改 runner 镜像；
#   - 目标前缀必须不存在：fail closed，不覆盖已有（含非空）前缀；
#   - 只走公开 https 源码克隆，不读取 GITHUB_TOKEN 等环境凭据；
#   - 不发布 bundle/镜像、不引入新包管理器，构建只在 workspace/scratch 内进行。
#
# 用法：
#   scripts/prepare_resource_deps.sh <empty-prefix-root> [--jobs N]
#   scripts/prepare_resource_deps.sh --print-cache-key
#   scripts/prepare_resource_deps.sh --help
#
set -euo pipefail

# ---------------------------------------------------------------------------
# 锁定来源（禁止猜测；改动必须与 docs/dependencies/resource-prefixes.provenance.md
# 以及 decisions 0003/0004 同步，并重新走构建 + 消费者验收）
# ---------------------------------------------------------------------------
HIREDIS_REPO="https://github.com/redis/hiredis"
HIREDIS_TAG="v1.4.1"
HIREDIS_COMMIT="616f2286ba5503f74ae96e720623fa11dbc690af"
HIREDIS_VERSION="1.4.0"          # 库内版本（与 tag 不一致，两者都记录）

MONGOC_REPO="https://github.com/mongodb/mongo-c-driver"
MONGOC_TAG="2.5.4"
MONGOC_COMMIT="ad87ab88907a0105823469fb5d393ed717bed9ba"
MONGOC_VERSION="2.5.4"

MONGOCXX_REPO="https://github.com/mongodb/mongo-cxx-driver"
MONGOCXX_TAG="r4.6.0"
MONGOCXX_COMMIT="5cfce9d754a91d47e996394e2cdc9f2e60403542"
MONGOCXX_VERSION="4.6.0"

# 确定性缓存键：只由锁定 commit 派生，与主机/时间无关，可直接作 actions/cache key 主体。
CACHE_KEY="bbt-resource-deps-hiredis-${HIREDIS_COMMIT:0:8}-mongoc-${MONGOC_COMMIT:0:8}-mongocxx-${MONGOCXX_COMMIT:0:8}"

JOBS_DEFAULT=$(( $(nproc) / 4 ))
[ "$JOBS_DEFAULT" -lt 2 ] && JOBS_DEFAULT=2

usage() {
    cat <<'EOF'
用法：
  scripts/prepare_resource_deps.sh <empty-prefix-root> [--jobs N]
      从固定源码构建 hiredis/mongoc/mongocxx 到 <empty-prefix-root> 下
      （hiredis/、mongoc/、mongocxx/ 子目录），并写出 manifest 与缓存键。
      <empty-prefix-root> 必须不存在；已存在（不论是否为空、是否为悬浮符号链接）
      一律拒绝。
  scripts/prepare_resource_deps.sh --print-cache-key
      只打印确定性缓存键后退出（不产生任何副作用），供 actions/cache key 使用。
  scripts/prepare_resource_deps.sh --help

环境：TMPDIR 可覆盖下载/构建临时目录（默认系统临时目录），默认并入最终前缀前的
      staging 与源码目录都随脚本退出清理；脚本不读取任何凭据类环境变量。
      ABI 证据由 objdump 生成，缺少 objdump 即 FATAL（不写 unknown）。
EOF
}

PREFIX=""
JOBS="$JOBS_DEFAULT"
while [ $# -gt 0 ]; do
    case "$1" in
        --print-cache-key) printf '%s\n' "$CACHE_KEY"; exit 0 ;;
        --jobs)
            [ $# -ge 2 ] || { echo "[resource-deps] FATAL: --jobs 需要参数" >&2; exit 2; }
            JOBS="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        -*)
            echo "[resource-deps] FATAL: 未知选项: $1" >&2; usage >&2; exit 2 ;;
        *)
            [ -z "$PREFIX" ] || { echo "[resource-deps] FATAL: 多余参数: $1" >&2; exit 2; }
            PREFIX="$1"; shift ;;
    esac
done
case "$JOBS" in ''|*[!0-9]*) echo "[resource-deps] FATAL: --jobs 必须为正整数" >&2; exit 2 ;; esac
[ "$JOBS" -ge 1 ] || { echo "[resource-deps] FATAL: --jobs 必须为正整数" >&2; exit 2; }
[ -n "$PREFIX" ] || { echo "[resource-deps] FATAL: 缺少 <empty-prefix-root>" >&2; usage >&2; exit 2; }

# fail closed：目标前缀已存在即拒绝，绝不覆盖既有（含非空）前缀。
# 先判「调用方原始参数」：realpath -m 会把结尾的悬浮符号链接解析成它不存在的目标，
# 于是「该路径项本身已存在」这件事被遮掉，符号链接会被当成空位放过。
if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
    echo "[resource-deps] FATAL: 目标前缀已存在，拒绝覆盖: $PREFIX" >&2
    exit 2
fi
PREFIX="$(realpath -m "$PREFIX")"
# 再判规范化后的路径：原始参数是指向既有目录的符号链接时同样拒绝。
if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
    echo "[resource-deps] FATAL: 目标前缀已存在，拒绝覆盖: $PREFIX" >&2
    exit 2
fi

mkdir -p "$(dirname "$PREFIX")"
# 先以 mkdir 原子占用锁名，避免两个并发 recipe 同时把 staging 改名到同一前缀。
LOCK="${PREFIX}.lock"
STAGING=""
WORK=""
cleanup() {
    [ -z "$STAGING" ] || rm -rf "$STAGING"
    [ -z "$WORK" ] || rm -rf "$WORK"
    [ -z "$LOCK" ] || rm -rf "$LOCK"
}
if ! mkdir "$LOCK" 2>/dev/null; then
    echo "[resource-deps] FATAL: 目标前缀正在被其他 recipe 准备: $PREFIX" >&2
    exit 2
fi
# 锁目录创建成功后立即安装 trap，避免信号在构建初始化窗口内留下锁。
trap cleanup EXIT
# 除 EXIT 外也接住 INT/TERM/HUP：否则中断会在原地留下锁目录，把该前缀路径永久卡死
# （后续任何一次调用都会因「锁已存在」而拒绝）。
trap 'cleanup; exit 130' INT TERM HUP
STAGING="$(mktemp -d "${PREFIX}.tmp.XXXXXX")"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/bbt-resource-deps-src.XXXXXX")"

# 整个构建阶段在 scratch 工作目录内进行：某些上游（mongo-cxx-driver）的 install 会把
# 安装清单写到「调用者当前目录」，切换 CWD 保证脚本不污染调用者的工作树/仓库。
cd "$WORK"

LAUNCHER=()
USE_CCACHE=""
if command -v ccache >/dev/null 2>&1; then
    USE_CCACHE=1
    LAUNCHER=(-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache)
fi

# 安装产物写入指向「最终前缀」的 RUNPATH：三份前缀不自带系统安装路径，若不写会把
# 构建期（staging）路径烘进 .so，重命名后成为死路径。写入最终前缀后前缀在
# install_root 处自定位；前缀若被移动，消费方回退注入 LD_LIBRARY_PATH（见 provenance 文档）。
INSTALL_RPATH="$PREFIX/hiredis/lib;$PREFIX/mongoc/lib;$PREFIX/mongocxx/lib"

log() { printf '[resource-deps] %s\n' "$*"; }

# tag -> 完整 SHA 核验：clone 固定 tag 后 rev-parse HEAD 必须等于锁定 commit。
# 只看 tag 名不构成固定依赖；SHA 不符即 FATAL（tag 移动/被替换也能挡住）。
clone_locked() {
    local repo="$1" tag="$2" commit="$3" dest="$4" head
    log "clone ${repo} ${tag}"
    GIT_TERMINAL_PROMPT=0 \
        git -c credential.helper= clone --quiet --depth 1 --branch "$tag" "$repo" "$dest"
    head="$(git -C "$dest" rev-parse HEAD)"
    if [ "$head" != "$commit" ]; then
        echo "[resource-deps] FATAL: ${repo} tag ${tag} 解析到 ${head}，期望固定 commit ${commit}" >&2
        exit 3
    fi
    log "  ${tag} -> ${head} (锁定命中)"
}

# 从已安装前缀定位版本头（不同上游安装布局不同，用 find 而不硬编码单一路径）。
find_header() {
    local root="$1" f; shift
    f="$(find "$root" "$@" -print -quit 2>/dev/null)"
    if [ -z "$f" ]; then
        echo "[resource-deps] FATAL: 在前缀 ${root} 下找不到版本头（find $*）" >&2
        exit 4
    fi
    printf '%s' "$f"
}

# 从已安装前缀的头文件读取库内版本，与锁定值精确比对；缓存命中也不能绕过版本核验。
check_version() {
    local label="$1" header="$2" pattern="$3" expected="$4" actual
    actual="$(grep -E "$pattern" "$header" | grep -oE '[0-9]+' | paste -sd. -)"
    if [ "$actual" != "$expected" ]; then
        echo "[resource-deps] FATAL: ${label} 版本不符：实际='${actual}' 要求='${expected}' ($header)" >&2
        exit 4
    fi
    log "  ${label} 版本锁定命中: ${actual}"
}

# ---------------------------------------------------------------------------
# 构建并安装三份前缀
# ---------------------------------------------------------------------------
log "前缀根目录: $PREFIX"
log "并行度: --parallel ${JOBS}（nproc=$(nproc) / 4，最小 2）"
log "编译缓存: $( [ -n "$USE_CCACHE" ] && echo ccache || echo 未启用 )"

# --- hiredis（Redis 官方 C 客户端；decision 0003）---------------------------
clone_locked "$HIREDIS_REPO" "$HIREDIS_TAG" "$HIREDIS_COMMIT" "$WORK/hiredis"
cmake -S "$WORK/hiredis" -B "$WORK/hiredis/build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$STAGING/hiredis" \
    -DBUILD_SHARED_LIBS=ON \
    -DDISABLE_TESTS=ON \
    -DCMAKE_INSTALL_RPATH="$INSTALL_RPATH" \
    "${LAUNCHER[@]+"${LAUNCHER[@]}"}"
cmake --build "$WORK/hiredis/build" --parallel "$JOBS"
cmake --install "$WORK/hiredis/build" >/dev/null
check_version hiredis \
    "$(find_header "$STAGING/hiredis/include" -name hiredis.h)" \
    '^#define HIREDIS_(MAJOR|MINOR|PATCH)' "$HIREDIS_VERSION"

# --- mongo-c-driver（decision 0004）-----------------------------------------
clone_locked "$MONGOC_REPO" "$MONGOC_TAG" "$MONGOC_COMMIT" "$WORK/mongoc"
cmake -S "$WORK/mongoc" -B "$WORK/mongoc/build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$STAGING/mongoc" \
    -DENABLE_MONGOC=ON \
    -DBUILD_TESTING=OFF \
    -DENABLE_EXAMPLES=OFF \
    -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
    -DENABLE_SSL=OFF \
    -DENABLE_MONGODB_AWS_AUTH=OFF \
    -DCMAKE_INSTALL_RPATH="$INSTALL_RPATH" \
    "${LAUNCHER[@]+"${LAUNCHER[@]}"}"
cmake --build "$WORK/mongoc/build" --parallel "$JOBS"
cmake --install "$WORK/mongoc/build" >/dev/null
check_version mongoc \
    "$(find_header "$STAGING/mongoc/include" -name mongoc-version.h)" \
    '^#define MONGOC_(MAJOR|MINOR|MICRO)_VERSION' "$MONGOC_VERSION"

# --- mongo-cxx-driver（decision 0004；bsoncxx 随同前缀）---------------------
clone_locked "$MONGOCXX_REPO" "$MONGOCXX_TAG" "$MONGOCXX_COMMIT" "$WORK/mongocxx"
cmake -S "$WORK/mongocxx" -B "$WORK/mongocxx/build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$STAGING/mongocxx" \
    -DCMAKE_PREFIX_PATH="$STAGING/mongoc" \
    -DBUILD_TESTING=OFF \
    -DENABLE_EXAMPLES=OFF \
    -DBSONCXX_LINK_WITH_STATIC_MONGOC=OFF \
    -DMONGOCXX_LINK_WITH_STATIC_MONGOC=OFF \
    -DCMAKE_INSTALL_RPATH="$INSTALL_RPATH" \
    "${LAUNCHER[@]+"${LAUNCHER[@]}"}"
cmake --build "$WORK/mongocxx/build" --parallel "$JOBS"
cmake --install "$WORK/mongocxx/build" >/dev/null
check_version mongocxx \
    "$(find_header "$STAGING/mongocxx/include" -path '*mongocxx/v1/config/version.hpp')" \
    '^#define MONGOCXX_VERSION_(MAJOR|MINOR|PATCH)' "$MONGOCXX_VERSION"

# ---------------------------------------------------------------------------
# manifest：SHA/版本 + 实际库来源（安装前缀内真实 .so 的相对名、realpath、sha256）
# ---------------------------------------------------------------------------

# JSON 字符串转义：install_root 与 toolchain 文本来自调用方环境（路径里可能有引号或
# 反斜杠），不转义会产出无法解析的 manifest，而消费侧是把它当证据读的。
json_escape() {
    local s="$1"
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    s="${s//$'\n'/\\n}"
    s="${s//$'\r'/\\r}"
    s="${s//$'\t'/\\t}"
    printf '%s' "$s"
}

json_libs() {
    local libdir="$1" first=1 f real sha
    printf '[\n'
    while IFS= read -r -d '' f; do
        real="$(readlink -f "$f")"
        sha="$(sha256sum "$real" | awk '{print $1}')"
        [ "$first" -eq 1 ] || printf ',\n'
        first=0
        printf '      {"name": "%s", "realpath": "%s", "sha256": "%s"}' \
            "$(json_escape "$(basename "$f")")" "$(json_escape "$(basename "$real")")" "$sha"
    done < <(find "$libdir" -maxdepth 1 \( -type f -o -type l \) -name '*.so*' -print0 2>/dev/null | sort -z)
    printf '\n    ]'
}

# ABI 证据必须是真实读到的符号版本：objdump 缺失、产物缺失或读符号表失败一律 fail
# closed，绝不用 "unknown" 冒充「已记录」的证据（消费侧只看见字段存在就会当它可用）。
command -v objdump >/dev/null 2>&1 || {
    echo "[resource-deps] FATAL: 缺少 objdump，无法生成 ABI 证据（不写 unknown）" >&2
    exit 6
}
abi_fatal() { echo "[resource-deps] FATAL: $*" >&2; exit 6; }

# 记录产物实际需要的最低运行时符号版本；镜像构建/消费侧不得只看源码版本。
max_symbol_version() {
    local label="$1" pattern="$2" f; shift 2
    local -a libs=()
    while IFS= read -r -d '' f; do libs+=("$f"); done \
        < <(find "$@" -maxdepth 1 \( -type f -o -type l \) -name '*.so*' -print0 2>/dev/null | sort -z)
    [ "${#libs[@]}" -gt 0 ] || abi_fatal "${label}: $* 下没有可读的 *.so* 产物"
    local dyn result
    dyn="$(objdump -T "${libs[@]}" 2>/dev/null)" \
        || abi_fatal "${label}: objdump -T 读取 ${#libs[@]} 个产物失败"
    result="$(printf '%s\n' "$dyn" | grep -oE "$pattern" | sort -Vu | tail -1)"
    [ -n "$result" ] || abi_fatal "${label}: 产物符号表里没有匹配 ${pattern} 的符号版本"
    printf '%s' "$result"
}

CI_CXX="$(g++ --version 2>/dev/null | head -1 || echo unknown)"
CMAKE_VER="$(cmake --version 2>/dev/null | head -1 || echo unknown)"
GEN_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ABI_LIB_DIRS=("$STAGING/hiredis/lib" "$STAGING/mongoc/lib" "$STAGING/mongocxx/lib")
GLIBC_REQUIRED="$(max_symbol_version glibc 'GLIBC_[0-9]+(\.[0-9]+)*' "${ABI_LIB_DIRS[@]}")"
GLIBCXX_REQUIRED="$(max_symbol_version glibcxx 'GLIBCXX_[0-9]+(\.[0-9]+)*' "${ABI_LIB_DIRS[@]}")"

{
    printf '{\n'
    printf '  "schema": "bbt-resource-deps/v1",\n'
    printf '  "generated_by": "bbtools-infra scripts/prepare_resource_deps.sh",\n'
    printf '  "generated_at_utc": "%s",\n' "$GEN_TS"
    printf '  "cache_key": "%s",\n' "$CACHE_KEY"
    printf '  "install_root": "%s",\n' "$(json_escape "$PREFIX")"
    printf '  "cmake_prefix_paths": ["%s", "%s", "%s"],\n' \
        "$(json_escape "$PREFIX/hiredis")" "$(json_escape "$PREFIX/mongoc")" "$(json_escape "$PREFIX/mongocxx")"
    printf '  "toolchain": {"compiler": "%s", "cmake": "%s"},\n' \
        "$(json_escape "$CI_CXX")" "$(json_escape "$CMAKE_VER")"
    printf '  "abi_requirements": {"glibc": "%s", "glibcxx": "%s"},\n' \
        "$GLIBC_REQUIRED" "$GLIBCXX_REQUIRED"
    printf '  "components": [\n'
    printf '    {\n'
    printf '      "name": "hiredis", "role": "redis-client",\n'
    printf '      "upstream": "%s", "tag": "%s", "commit": "%s",\n' \
        "$HIREDIS_REPO" "$HIREDIS_TAG" "$HIREDIS_COMMIT"
    printf '      "installed_version": "%s", "install_subdir": "hiredis",\n' "$HIREDIS_VERSION"
    printf '      "cmake_package_targets": ["hiredis::hiredis"],\n'
    printf '      "libs": %s\n' "$(json_libs "$STAGING/hiredis/lib")"
    printf '    },\n'
    printf '    {\n'
    printf '      "name": "mongoc", "role": "mongo-c-driver",\n'
    printf '      "upstream": "%s", "tag": "%s", "commit": "%s",\n' \
        "$MONGOC_REPO" "$MONGOC_TAG" "$MONGOC_COMMIT"
    printf '      "installed_version": "%s", "install_subdir": "mongoc",\n' "$MONGOC_VERSION"
    printf '      "cmake_package_targets": ["mongoc::shared", "bson::shared"],\n'
    printf '      "libs": %s\n' "$(json_libs "$STAGING/mongoc/lib")"
    printf '    },\n'
    printf '    {\n'
    printf '      "name": "mongocxx", "role": "mongo-cxx-driver",\n'
    printf '      "upstream": "%s", "tag": "%s", "commit": "%s",\n' \
        "$MONGOCXX_REPO" "$MONGOCXX_TAG" "$MONGOCXX_COMMIT"
    printf '      "installed_version": "%s", "install_subdir": "mongocxx",\n' "$MONGOCXX_VERSION"
    printf '      "cmake_package_targets": ["mongo::mongocxx_shared", "mongo::bsoncxx_shared"],\n'
    printf '      "libs": %s\n' "$(json_libs "$STAGING/mongocxx/lib")"
    printf '    }\n'
    printf '  ]\n'
    printf '}\n'
} > "$STAGING/resource-deps.manifest.json"

# 原子的最后一步：staging 就位后再改名，避免半成品前缀被当成已完成。
# -T 禁止把 staging 嵌套进一个意外出现的同名目录；锁目录则挡住本 recipe 的并发调用。
# rename(2) 会替换「已存在的空目录」，所以改名瞬间再核一次存在性：出现即拒绝覆盖。
if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
    echo "[resource-deps] FATAL: 准备期间目标前缀已出现，拒绝覆盖: $PREFIX" >&2
    exit 5
fi
mv -T "$STAGING" "$PREFIX"
[ ! -e "$STAGING" ] || {
    echo "[resource-deps] FATAL: staging 未完成迁移: $STAGING" >&2
    exit 5
}
[ -f "$PREFIX/resource-deps.manifest.json" ] &&
[ -d "$PREFIX/hiredis" ] &&
[ -d "$PREFIX/mongoc" ] &&
[ -d "$PREFIX/mongocxx" ] || {
    echo "[resource-deps] FATAL: 最终前缀完整性校验失败: $PREFIX" >&2
    exit 5
}
rm -rf "$LOCK"
LOCK=""
trap - EXIT INT TERM HUP
rm -rf "$WORK"
WORK=""

log "完成。前缀："
log "  BBT_HIREDIS_PREFIX=$PREFIX/hiredis"
log "  BBT_MONGOC_PREFIX=$PREFIX/mongoc"
log "  BBT_MONGOCXX_PREFIX=$PREFIX/mongocxx"
log "  manifest=$PREFIX/resource-deps.manifest.json"
log "  cache_key=$CACHE_KEY"
