#!/bin/bash
# hermes-studio 容器入口（bridge 架构合并版）
#
# - 设了 BRIDGE_GATEWAY_URL → 后台拉起内置 bridge（连你的 hermes gateway），
#   studio 以 tcp://127.0.0.1:$BRIDGE_LISTEN_PORT 连接（容器内 localhost）
# - 未设 → 纯净模式，行为与原镜像一致（无 agent，bridge 不启动）
set -u

BRIDGE_LISTEN_PORT="${BRIDGE_LISTEN_PORT:-18765}"
BRIDGE_STATE_FILE="${BRIDGE_STATE_FILE:-${HERMES_HOME:-/home/agent/.hermes}/bridge_state.json}"

if [ -n "${BRIDGE_GATEWAY_URL:-}" ]; then
  echo "[entrypoint] bridge 模式：gateway=${BRIDGE_GATEWAY_URL}" >&2
  mkdir -p "$(dirname "$BRIDGE_STATE_FILE")"
  (
    while true; do
      python3 /opt/bridge/bridge.py \
        --listen "127.0.0.1:${BRIDGE_LISTEN_PORT}" \
        --gateway "${BRIDGE_GATEWAY_URL}" \
        ${BRIDGE_GATEWAY_TOKEN:+--token "${BRIDGE_GATEWAY_TOKEN}"} \
        --state-file "${BRIDGE_STATE_FILE}"
      echo "[entrypoint] bridge 退出，1s 后重启" >&2
      sleep 1
    done
  ) &
  export HERMES_AGENT_BRIDGE_ENDPOINT="tcp://127.0.0.1:${BRIDGE_LISTEN_PORT}"
  export HERMES_BIN="${HERMES_BIN:-/usr/local/bin/hermes}"
  export HERMES_WEB_UI_DISABLE_GATEWAY_AUTOSTART=1
fi

exec /app/bin/start-studio-all.sh "$@"