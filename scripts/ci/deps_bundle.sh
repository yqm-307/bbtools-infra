#!/usr/bin/env bash
# 依赖归档 pack/unpack：把 coroutine/core 源码与 hiredis/mongoc/mongocxx 私有前缀
# 打成**单一 tar**，供 Release/Debug job 复用同一份 prepared deps。
#
# 关键约束（对应本轮 15min 预算修复）：
#   - tar 默认把符号链接存成符号链接（不 dereference）；artifact 只承载 tar 字节，
#     所以不会经 actions/upload-artifact 的目录符号链接语义把私有依赖前缀的 symlink 丢/展开。
#   - meta 记录打包时 workspace 绝对路径与 tar sha256（完整性校验）；
#     上游 SHAs 由 prepare job 输出，消费 job 以 expected sha 锚定同一归档。
#   - unpack 必须校验 sha256，不匹配即 fail-closed（不做弱 TLS、不静默回退）。
#   - workspace 与打包时不同时，先检查前缀 cmake/pkg-config 是否把打包路径写死；
#     写死则拒绝解包（不产出静默损坏的消费端）。
#
# 用法：
#   scripts/ci/deps_bundle.sh pack   --root <ws> --out <tar> --meta <meta>
#   scripts/ci/deps_bundle.sh unpack --tar <tar> --meta <meta> --dest <ws>
set -euo pipefail

# 打包进 tar 的相对成员（相对 --root）；解包到同一相对路径即绝对路径一致
MEMBERS=(coroutine bbtools-core deps-prefix)

usage() { sed -n '2,20p' "$0"; }

emit() {  # key=value -> $GITHUB_OUTPUT（离线时 stdout）
  if [ -n "${GITHUB_OUTPUT:-}" ]; then printf '%s\n' "$1" >> "$GITHUB_OUTPUT"; else printf '%s\n' "$1"; fi
}

pack() {
  local root="" out="" meta=""
  while [ $# -gt 0 ]; do case "$1" in
    --root) root="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --meta) meta="$2"; shift 2 ;;
    *) echo "[deps-bundle] FATAL: 未知参数 $1" >&2; exit 2 ;;
  esac; done
  [ -n "$root" ] && [ -n "$out" ] && [ -n "$meta" ] \
    || { echo "[deps-bundle] FATAL: pack 缺 --root/--out/--meta" >&2; exit 2; }
  local m
  for m in "${MEMBERS[@]}"; do
    [ -e "$root/$m" ] || { echo "[deps-bundle] FATAL: 缺成员 $root/$m" >&2; exit 2; }
  done
  tar -cf "$out" -C "$root" "${MEMBERS[@]}"
  local sha nsym
  sha="$(sha256sum "$out" | awk '{print $1}')"
  nsym="$(tar -tvf "$out" | awk '$1 ~ /^l/ {c++} END{print c+0}')"
  {
    echo "workspace=$root"
    echo "sha256=$sha"
    echo "symlinks=$nsym"
    echo "members=${MEMBERS[*]}"
  } > "$meta"
  emit "bundle_sha256=$sha"
  echo "[deps-bundle] 打包完成: $out (sha256=$sha, symlinks=$nsym)"
}

unpack() {
  local tar="" meta="" dest="" expected="" expected_set=false
  while [ $# -gt 0 ]; do case "$1" in
    --tar) tar="$2"; shift 2 ;;
    --meta) meta="$2"; shift 2 ;;
    --dest) dest="$2"; shift 2 ;;
    --expect-sha) expected="$2"; expected_set=true; shift 2 ;;
    *) echo "[deps-bundle] FATAL: 未知参数 $1" >&2; exit 2 ;;
  esac; done
  [ -n "$tar" ] && [ -n "$meta" ] && [ -n "$dest" ] \
    || { echo "[deps-bundle] FATAL: unpack 缺 --tar/--meta/--dest" >&2; exit 2; }
  [ -f "$tar" ] || { echo "[deps-bundle] FATAL: tar 不存在 $tar" >&2; exit 1; }
  [ -f "$meta" ] || { echo "[deps-bundle] FATAL: meta 不存在 $meta" >&2; exit 1; }
  local want got packed_root
  want="$(sed -n 's/^sha256=//p' "$meta")"
  got="$(sha256sum "$tar" | awk '{print $1}')"
  [[ "$want" =~ ^[0-9a-f]{64}$ ]] || { echo "[deps-bundle] FATAL: meta sha256 缺失或非法" >&2; exit 1; }
  if $expected_set; then
    [[ "$expected" =~ ^[0-9a-f]{64}$ ]] && [ "$expected" = "$want" ] \
      || { echo "[deps-bundle] FATAL: prepare job 的 expected sha 缺失或不匹配" >&2; exit 1; }
  fi
  if [ "$want" != "$got" ]; then
    echo "[deps-bundle] FATAL: tar sha256 不匹配 (want=$want got=$got)：归档损坏/缺失，拒绝弱回退" >&2
    exit 1
  fi
  packed_root="$(sed -n 's/^workspace=//p' "$meta")"
  [[ "$packed_root" = /* && "$packed_root" != *$'\n'* ]] \
    || { echo "[deps-bundle] FATAL: meta workspace 缺失或非法" >&2; exit 1; }
  tar -xf "$tar" -C "$dest"
  if [ "$packed_root" != "$dest" ]; then
    # 完整消费 grep 输出：不能用 head 提前退出，让 SIGPIPE 把匹配误判成无匹配。
    # grep=1 表示无匹配，其他错误一律拒绝，不用 || true 吞掉读取失败。
    local hits rc=0
    hits="$(grep -rIlF --include='*.cmake' -- "$packed_root" "$dest/deps-prefix")" || rc=$?
    [ "$rc" -le 1 ] || { echo "[deps-bundle] FATAL: 无法核验前缀 CMake config" >&2; exit 1; }
    if [ -n "$hits" ]; then
      echo "[deps-bundle] FATAL: 前缀 CMake config 写死了打包路径 $packed_root，当前 dest=$dest 不同；fail-closed 拒绝" >&2
      exit 1
    fi
    echo "[deps-bundle] WARN: workspace 与打包时不同（*.cmake 未写死路径）；运行时库使用消费端私有前缀" >&2
  fi
  # .so 的旧 RUNPATH 不随前缀搬迁；后续 ctest/ldd 统一读取消费者目录。
  local loader="" module subdir
  for module in hiredis mongoc mongocxx; do
    for subdir in lib lib64; do
      [ -d "$dest/deps-prefix/$module/$subdir" ] || continue
      loader="${loader:+$loader:}$dest/deps-prefix/$module/$subdir"
    done
  done
  [ -n "$loader" ] || { echo "[deps-bundle] FATAL: 无消费端库目录" >&2; exit 1; }
  loader="$loader${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  if [ -n "${GITHUB_ENV:-}" ]; then
    printf 'LD_LIBRARY_PATH=%s\n' "$loader" >> "$GITHUB_ENV"
  else
    printf 'LD_LIBRARY_PATH=%s\n' "$loader"
  fi
  echo "[deps-bundle] 解包完成 -> $dest (sha256 ok)"
}

cmd="${1:-}"
[ $# -gt 0 ] && shift || true
case "$cmd" in
  pack) pack "$@" ;;
  unpack) unpack "$@" ;;
  -h|--help|"") usage; exit 0 ;;
  *) echo "[deps-bundle] FATAL: 未知子命令 $cmd" >&2; exit 2 ;;
esac
