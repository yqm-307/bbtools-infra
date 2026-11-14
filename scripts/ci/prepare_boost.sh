#!/usr/bin/env bash
#
# 影子 hosted 工具链配方：源码构建**锁定版 Boost 1.90**到给定私有前缀。
#
# 为什么需要它：现役 ci.yml 运行在 ARC 镜像内，镜像自带 /opt/boost（Boost 1.90），
# hosted runner 只有系统预装 Boost（版本不同）。按 Issue #50/R3「工具链必须按实际
# 需求固定/复现，不能用 hosted 预装版本假替代」，此处把锁定版 Boost 真的编到工作区
# 前缀，不改 runner 镜像、不装系统包、不写 /usr/local。
#
# 锁定输入来自 framework 已独立审查发布的 docker/toolchain.lock（BOOST_VERSION /
# BOOST_SHA256），与 bbt-framework docker/bbtools-common-image 的 Boost 配方同源同值：
#   BOOST_VERSION=1.90.0
#   BOOST_SHA256=5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9
# 下载后必须 sha256 命中，否则 fail-closed。
#
# 可选环境变量 BOOST_SOURCE_CACHE：源码归档目录（不是解包目录/前缀）。命中时复用该
# 归档，但每次仍按上面的完整 SHA256 校验；缺失/损坏/symlink 一律退回下载，回写失败
# 只告警。缓存故障绝不跳过校验，也不复用任何已安装 prefix 或 build 树。
#
# 编译组件：只编 gate 实际消费的 `context`（现役真实 CI 日志实证
# `-- Found Boost: .../Boost-1.90.0/BoostConfig.cmake ... found components: context`）。
# 若将来实际需求新增 Boost 编译组件，在 BOOST_LIBS 追加并同步验证，不预先扩编。
#
# 用法：scripts/ci/prepare_boost.sh <empty-prefix> [--jobs N] [--print-plan]
#   <empty-prefix> 必须不存在；已存在（含空目录/悬浮符号链接）一律拒绝。
#   --print-plan    只打印将下载的 URL/校验值/组件，不产生任何副作用（离线冒烟用）。
set -euo pipefail

BOOST_VERSION="1.90.0"
BOOST_SHA256="5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9"
BOOST_LIBS="context"
BOOST_VERSION_NUM=109000  # 1*100000 + 90*100 + 0
BOOST_SOURCE_CACHE="${BOOST_SOURCE_CACHE:-}"

JOBS_DEFAULT=$(( $(nproc) / 4 )); [ "$JOBS_DEFAULT" -lt 2 ] && JOBS_DEFAULT=2

V_US="${BOOST_VERSION//./_}"
ARCHIVE_URL="https://archives.boost.io/release/${BOOST_VERSION}/source/boost_${V_US}.tar.gz"

usage() {
    cat <<'EOF'
用法：scripts/ci/prepare_boost.sh <empty-prefix> [--jobs N] [--print-plan]
  <empty-prefix>  目标安装前缀；必须不存在（fail-closed，不覆盖）。
  --jobs N        并行度，默认 nproc/4（最小 2）。
  --print-plan    只打印计划（URL/sha/组件），无副作用。
环境变量 BOOST_SOURCE_CACHE（可选）指定源码归档缓存目录；每次仍做完整 SHA256 校验。
EOF
}

log() { printf '[prepare-boost] %s\n' "$*"; }

# 只在归档通过完整 SHA256 校验后进入缓存；任何缓存异常都退回普通下载，不影响构建。
# 之所以校验而不是信任缓存：缓存内容不签名，只有锁定 SHA256 才是内容锚点。
acquire_boost_archive() {
    local work="$1" dest="$1/boost.tar.gz" cache="" hit=0 tmp=""

    if [ -n "$BOOST_SOURCE_CACHE" ]; then
        if [ -L "$BOOST_SOURCE_CACHE" ] || { [ -e "$BOOST_SOURCE_CACHE" ] && [ ! -d "$BOOST_SOURCE_CACHE" ]; }; then
            log "源码缓存目录不可用（symlink/非目录）：按冷构建下载"
        else
            cache="$BOOST_SOURCE_CACHE/boost_${V_US}.tar.gz"
        fi
    fi

    if [ -n "$cache" ] && [ -e "$cache" ]; then
        if [ -L "$cache" ]; then
            log "源码缓存条目是 symlink，拒绝使用：按冷构建下载"
        elif [ -f "$cache" ] && cp -- "$cache" "$dest" 2>/dev/null \
             && printf '%s  %s\n' "$BOOST_SHA256" "$dest" | sha256sum --check --status; then
            log "命中源码缓存：$cache（完整 sha256 校验通过）"
            hit=1
        else
            log "源码缓存条目损坏（sha256 不符），丢弃并重新下载"
            rm -f -- "$dest"
        fi
    fi

    if [ "$hit" = 0 ]; then
        log "下载 $ARCHIVE_URL（sha256 校验 fail-closed）"
        rm -f -- "$dest"
        curl --fail --location --retry 3 --retry-all-errors --silent --show-error \
            "$ARCHIVE_URL" -o "$dest" \
            || { echo "[prepare-boost] FATAL: Boost 归档下载失败" >&2; return 3; }
        printf '%s  %s\n' "$BOOST_SHA256" "$dest" | sha256sum --check --status \
            || { echo "[prepare-boost] FATAL: Boost 归档 sha256 不符（拒绝继续）" >&2; return 3; }

        if [ -n "$cache" ]; then
            if mkdir -p -- "$BOOST_SOURCE_CACHE" 2>/dev/null \
               && [ -d "$BOOST_SOURCE_CACHE" ] && [ ! -L "$BOOST_SOURCE_CACHE" ]; then
                tmp="$BOOST_SOURCE_CACHE/.boost_${V_US}.tar.gz.tmp.$$"
                if cp -- "$dest" "$tmp" 2>/dev/null && mv -f -- "$tmp" "$cache" 2>/dev/null; then
                    log "回写源码缓存：$cache"
                else
                    rm -f -- "$tmp" 2>/dev/null || true
                    log "源码缓存回写失败（忽略，不影响构建）"
                fi
            else
                log "源码缓存目录不可写（忽略，不影响构建）"
            fi
        fi
    fi
    return 0
}

PREFIX=""
JOBS="$JOBS_DEFAULT"
PRINT_PLAN=0

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --jobs)
                [ $# -ge 2 ] || { echo "[prepare-boost] FATAL: --jobs 需要参数" >&2; exit 2; }
                JOBS="$2"; shift 2 ;;
            --print-plan) PRINT_PLAN=1; shift ;;
            -h|--help) usage; exit 0 ;;
            -*) echo "[prepare-boost] FATAL: 未知选项: $1" >&2; usage >&2; exit 2 ;;
            *) [ -z "$PREFIX" ] || { echo "[prepare-boost] FATAL: 多余参数: $1" >&2; exit 2; }
               PREFIX="$1"; shift ;;
        esac
    done
    case "$JOBS" in ''|*[!0-9]*) echo "[prepare-boost] FATAL: --jobs 必须为正整数" >&2; exit 2 ;; esac
    [ "$JOBS" -ge 1 ] || { echo "[prepare-boost] FATAL: --jobs 必须为正整数" >&2; exit 2; }
}

main() {
    parse_args "$@"

    if [ "$PRINT_PLAN" = 1 ]; then
        printf 'boost_version=%s\n' "$BOOST_VERSION"
        printf 'boost_sha256=%s\n' "$BOOST_SHA256"
        printf 'boost_libs=%s\n' "$BOOST_LIBS"
        printf 'boost_version_num=%s\n' "$BOOST_VERSION_NUM"
        printf 'archive_url=%s\n' "$ARCHIVE_URL"
        exit 0
    fi

    [ -n "$PREFIX" ] || { echo "[prepare-boost] FATAL: 缺少 <empty-prefix>" >&2; usage >&2; exit 2; }

    # fail-closed：目标前缀已存在即拒绝，绝不覆盖。
    if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
        echo "[prepare-boost] FATAL: 目标前缀已存在，拒绝覆盖: $PREFIX" >&2
        exit 2
    fi
    PREFIX="$(realpath -m "$PREFIX")"
    if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
        echo "[prepare-boost] FATAL: 目标前缀已存在，拒绝覆盖: $PREFIX" >&2
        exit 2
    fi

    STAGING=""
    WORK=""
    cleanup() { [ -z "$STAGING" ] || rm -rf "$STAGING"; [ -z "$WORK" ] || rm -rf "$WORK"; }
    trap cleanup EXIT INT TERM HUP

    mkdir -p "$(dirname "$PREFIX")"
    STAGING="$(mktemp -d "${PREFIX}.tmp.XXXXXX")"
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/bbt-boost-src.XXXXXX")"

    acquire_boost_archive "$WORK"

    mkdir -p "$WORK/boost"
    tar xzf "$WORK/boost.tar.gz" -C "$WORK/boost" --strip-components=1
    cd "$WORK/boost"
    log "bootstrap（--with-libraries=$BOOST_LIBS）"
    ./bootstrap.sh --prefix="$STAGING" --with-libraries="$BOOST_LIBS" >/dev/null
    log "b2 install（-j$JOBS, release, shared+static）"
    ./b2 -j"$JOBS" variant=release link=shared,static threading=multi install >/dev/null

    # 版本身份取自实际 version.hpp 内容，不由 lock/echo 自证。
    VH="$STAGING/include/boost/version.hpp"
    [ -f "$VH" ] || { echo "[prepare-boost] FATAL: 缺 Boost 版本头: $VH" >&2; exit 4; }
    GOT="$(awk '$1=="#define" && $2=="BOOST_VERSION"{print $3; exit}' "$VH")"
    [ "$GOT" = "$BOOST_VERSION_NUM" ] \
        || { echo "[prepare-boost] FATAL: Boost 版本不符: $VH BOOST_VERSION=$GOT 期望 $BOOST_VERSION_NUM" >&2; exit 4; }
    [ -e "$STAGING/lib/libboost_context.so" ] \
        || { echo "[prepare-boost] FATAL: 缺 libboost_context.so（实际消费组件）" >&2; exit 4; }

    # staging 就位后再原子改名，避免半成品前缀被当成已完成。
    if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
        echo "[prepare-boost] FATAL: 准备期间目标前缀已出现，拒绝覆盖: $PREFIX" >&2
        exit 5
    fi
    mv -T "$STAGING" "$PREFIX"
    STAGING=""
    log "完成：BOOST_ROOT=$PREFIX（Boost $BOOST_VERSION，components=$BOOST_LIBS）"
}

# 只有直接执行才跑 main；被测试 source 时只暴露函数（便于直接耦合测试缓存分支）。
if [ "${BASH_SOURCE[0]:-$0}" = "$0" ]; then
    main "$@"
fi
