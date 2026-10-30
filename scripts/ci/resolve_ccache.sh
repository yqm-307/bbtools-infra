#!/usr/bin/env bash
# 解析本次 run 内 ccache launcher 的启用条件（跨 run 缓存已按 hosted 阻塞）。
# 供 workflow 步骤输出 enabled=true|false 到 $GITHUB_OUTPUT。
#
# 输入（env）：
#   EVENT_NAME           github.event_name
#   HEAD_REPO            github.event.pull_request.head.repo.full_name
#   REPO                 github.repository
#   CACHE_ENABLED        workflow env CACHE_ENABLED（"true"|"false"）
#   GITHUB_OUTPUT        可选；未设置时写 stdout
set -euo pipefail

echo "跨 run ccache/GitHub cache 已按 hosted 明确阻塞：不 restore/save。"
if [ "${EVENT_NAME:-}" = "pull_request" ] && \
   [ "${HEAD_REPO:-}" != "${REPO:-}" ]; then
    echo "fork PR：不启用 ccache launcher"
    echo "enabled=false" >> "${GITHUB_OUTPUT:-/dev/stdout}"
elif [ "${CACHE_ENABLED:-}" != "true" ]; then
    echo "cache-bypass / cache-enabled=false：不启用 ccache launcher"
    echo "enabled=false" >> "${GITHUB_OUTPUT:-/dev/stdout}"
elif ! command -v ccache >/dev/null 2>&1; then
    echo "runner 未安装 ccache：不启用 ccache launcher（不假定镜像自带）"
    echo "enabled=false" >> "${GITHUB_OUTPUT:-/dev/stdout}"
else
    echo "enabled=true" >> "${GITHUB_OUTPUT:-/dev/stdout}"
fi
