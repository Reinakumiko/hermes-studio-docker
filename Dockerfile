# syntax=docker/dockerfile:1
#
# 干净的 hermes-studio（hermes-web-ui）镜像 —— 不内置 hermes agent。
#
# 与上游官方 Dockerfile 的区别：
#   1. 上游基于 nousresearch/hermes-agent 基础镜像（内含完整 agent 运行时 + Python）；
#      本镜像基于纯净的 node:24-bookworm-slim，不含 agent。
#   2. 多阶段构建：编译工具只留在 builder 阶段，运行阶段只保留运行所需。
#   3. ffmpeg 可选：默认不装（镜像更小）；需要浏览器语音输入转码时，
#      构建时传 --build-arg WITH_FFMPEG=true 会下载静态 ffmpeg（约 76MB，无依赖）。
#
# 构建（在 workflow 里由 Actions 执行）：
#   docker build --build-arg WITH_FFMPEG=true -t hermes-studio:latest .

# ── 构建阶段：编译 hermes-studio（与上游官方 Dockerfile 相同的构建配方）──
FROM node:24-bookworm AS builder

WORKDIR /app

COPY package*.json ./
# 提高 Node 内存上限，防止构建时 OOM（上游同款）
ENV NODE_OPTIONS=--max-old-space-size=4096
RUN npm ci --ignore-scripts && npm rebuild node-pty

COPY . .
RUN npm run build \
    && npm prune --omit=dev \
    && npm run verify:sharp-runtime

# ── 运行阶段：干净的 hermes-studio ──
FROM node:24-bookworm-slim

# 是否附带静态 ffmpeg（浏览器语音输入转码用）。默认 false。
ARG WITH_FFMPEG=false
ARG FFMPEG_VERSION=b6.1.1

# 运行阶段系统依赖：
#   - ca-certificates : HTTPS（npm / 远端下载）
#   - libstdc++6      : sharp / sherpa-onnx 等原生模块需要 C++ 运行时
#   - curl            : 下载静态 ffmpeg 用（仅 WITH_FFMPEG=true 时需要）
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 curl \
    && rm -rf /var/lib/apt/lists/*

# 可选：下载对应架构的静态 ffmpeg（单文件、无依赖，约 76MB/48MB/30MB）
# 来源：ffmpeg-static 项目的 GitHub release（与 npm 包 ffmpeg-static 同源）
RUN if [ "$WITH_FFMPEG" = "true" ]; then \
      ARCH="$(dpkg --print-architecture)"; \
      case "$ARCH" in \
        amd64) FF="ffmpeg-linux-x64" ;; \
        arm64) FF="ffmpeg-linux-arm64" ;; \
        armhf) FF="ffmpeg-linux-arm" ;; \
        *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;; \
      esac; \
      echo "Downloading static ffmpeg ($FF) ..."; \
      curl -fsSL "https://github.com/eugeneware/ffmpeg-static/releases/download/${FFMPEG_VERSION}/${FF}.gz" -o /tmp/ffmpeg.gz \
      && gunzip -c /tmp/ffmpeg.gz > /usr/local/bin/ffmpeg \
      && chmod +x /usr/local/bin/ffmpeg \
      && rm -f /tmp/ffmpeg.gz \
      && ffmpeg -version | head -1; \
    fi

# 非 root 运行用户（与上游镜像内的路径约定保持一致：/home/agent）
RUN useradd --create-home --home-dir /home/agent --shell /bin/bash agent

# bridge 架构支持：hermes 探测 stub + 内置 bridge（单容器形态）。
# - hermes-studio 启动时用 Python import 探测 hermes 可用性（import hermes_cli），
#   探测不到就跳过 agent bridge 启动。本镜像不含真实 hermes，故内置最小 stub
#   让探测通过，实际 agent 由外部 gateway 提供。
# - bridge.py 一并打进镜像：设 BRIDGE_GATEWAY_URL 环境变量即启用（见 container-entrypoint.sh），
#   studio 经容器内 localhost 连接，无需独立 bridge 容器。
# - 设 WITH_BRIDGE_STUB=false 可得真正无 Python 的纯净镜像（无 bridge 能力）。
ARG WITH_BRIDGE_STUB=true
RUN if [ "$WITH_BRIDGE_STUB" = "true" ]; then \
      apt-get update \
      && apt-get install -y --no-install-recommends python3 python3-websockets \
      && rm -rf /var/lib/apt/lists/* \
      && mkdir -p /usr/local/lib/hermes-stub/hermes_cli \
      && printf '__version__ = "0.21.0-bridge-stub"\n' > /usr/local/lib/hermes-stub/hermes_cli/__init__.py \
      && printf '#!/usr/bin/env python3\nimport hermes_cli\nprint(f"Hermes Agent {hermes_cli.__version__}")\n' > /usr/local/bin/hermes \
      && chmod +x /usr/local/bin/hermes; \
    fi
# stub 目录对 PYTHONPATH 无害（不存在时 Python 忽略）
ENV PYTHONPATH=/usr/local/lib/hermes-stub

# 内置 bridge（来自 named context "bridge"，见 workflow 的 additional-contexts）
COPY --from=bridge --chown=agent:agent bridge.py /opt/bridge/bridge.py
COPY --from=bridge --chown=root:root container-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /app
# 只复制运行所需文件（与官方 npm 包 hermes-web-ui 的内容一致）
COPY --from=builder --chown=agent:agent /app/node_modules ./node_modules
COPY --from=builder --chown=agent:agent /app/dist ./dist
COPY --from=builder --chown=agent:agent /app/bin ./bin
COPY --from=builder --chown=agent:agent /app/package.json ./package.json

ENV NODE_ENV=production \
    HOME=/home/agent \
    HERMES_HOME=/home/agent/.hermes \
    PORT=6060 \
    NPM_CONFIG_PREFIX=/home/agent/.hermes-web-ui/coding-agent/npm \
    PATH=/home/agent/.hermes-web-ui/coding-agent/npm/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

USER agent
EXPOSE 6060
ENTRYPOINT ["/entrypoint.sh"]
CMD []