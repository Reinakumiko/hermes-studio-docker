# hermes-studio docker 镜像自动构建

在自己的 GitHub 账号下，自动构建 [EKKOLearnAI/hermes-studio](https://github.com/EKKOLearnAI/hermes-studio)（web UI 部分）的 Docker 镜像，并推送到 GHCR：`ghcr.io/<你的用户名>/<本仓库名>`。

**这是「干净」镜像**：只包含 hermes-studio web UI，**不内置 hermes agent**（上游官方镜像基于 `nousresearch/hermes-agent`，内含完整 agent 运行时 + Python，体积大）。本镜像基于 `node:24-bookworm-slim`，多阶段构建，非 root 运行。

**跟随上游 release 的方式**：GitHub Actions 无法订阅「别人仓库」的 release 事件，所以本仓库用**定时轮询**逼近——默认每 6 小时查一次上游 release，有新版本才构建（没有新版本时整次运行 <1 分钟即退出，几乎不消耗时间）。上游发版很频繁（每周 2~4 个），6 小时的检查间隔基本等于"跟随 release"。

## 快速开始

1. 在你的 GitHub 上创建一个空仓库（任意名字，比如 `hermes-studio`），把本目录推上去：

   ```bash
   git remote add origin git@github.com:<你的用户名>/<你的仓库>.git
   git push -u origin main
   ```

2. 推送即触发首次构建（workflow 对自身文件的 push 会触发一次），或者去 **Actions → Build Hermes Studio Docker Image → Run workflow** 手动触发。

3. 构建完成后，镜像在：`ghcr.io/<你的用户名>/<仓库名>`，tag 有三个：
   - `v0.7.20`（与上游 release tag 完全一致）
   - `0.7.20`（去掉 v 前缀）
   - `latest`（最新的 web-ui release）

## 触发方式

| 触发 | 行为 |
|---|---|
| `schedule`（每 6 小时） | 查询上游最新 web-ui release，GHCR 里没有对应 tag 才构建 |
| `push`（仅本 workflow 文件变动） | 同上，用于改完 workflow 后立即验证 |
| `workflow_dispatch`（手动） | 可选参数：`tag`（构建指定版本）、`platforms`（默认 `linux/amd64`）、`with_ffmpeg`（附带静态 ffmpeg，默认 false）、`force`（已存在也强制重建） |

想改成**每天一次**：把 `.github/workflows/docker-build.yml` 里的 cron `17 */6 * * *` 改成 `17 2 * * *`。

## 镜像大小与 ffmpeg 选项

| 配置 | 大小 | 说明 |
|---|---|---|
| 默认（无 ffmpeg） | **~863MB** | 最小干净版，语音输入转码不可用，其余全正常 |
| `with_ffmpeg: true` | **~940MB** | 附带静态 ffmpeg（+76MB 单文件），浏览器语音输入（WebM→WAV 转码）可用 |

- ffmpeg 用**静态二进制**（`ffmpeg-static` 项目的 GitHub release，单文件无依赖），不是 apt 全家桶（apt 版要 +475MB）。
- 手动触发时勾选 `with_ffmpeg` 即可构建带语音转码的版本。
- 想本地手动构建：`docker build --build-arg WITH_FFMPEG=true -t hermes-studio .`

## 拉取镜像

首次推送后，GHCR 里的包默认是 **private** 的（只对你自己可见）：

- 保持 private：在要拉取的机器上 `docker login ghcr.io`（用户名 = GitHub 用户名，密码 = 带 `read:packages` 权限的 PAT）
- 想匿名拉取：GitHub 仓库页右侧 **Packages → 该包 → Package settings → Change visibility → Public**

## 运行

```bash
# docker run（最小化）
docker run -d --name hermes-webui \
  -p 6060:6060 \
  -v ./hermes_data:/home/agent/.hermes \
  -v ./hermes_data/hermes-web-ui:/home/agent/.hermes-web-ui \
  ghcr.io/<你的用户名>/<你的仓库>:latest

# 或用本仓库的 docker-compose.yml
WEBUI_IMAGE=ghcr.io/<你的用户名>/<你的仓库>:latest docker compose up -d
```

打开 `http://localhost:6060`。首次启动会在日志里打印自动生成的 auth token：

```bash
docker logs hermes-webui 2>&1 | grep -i token
# 或 cat ./hermes_data/hermes-web-ui/.token
```

端口/环境变量的完整说明见上游文档 [docs/docker.md](https://github.com/EKKOLearnAI/hermes-studio/blob/main/docs/docker.md)。

## 实现说明

- **用本仓库的干净 Dockerfile 构建**：多阶段构建——builder 阶段（`node:24-bookworm`）用上游官方构建配方（`npm ci --ignore-scripts && npm rebuild node-pty && npm run build && npm prune --omit=dev`）编译；运行阶段（`node:24-bookworm-slim`）只保留运行所需（`libstdc++6` + 构建产物），非 root 用户 `agent`，入口 `node dist/server/index.js`，端口 6060。
- **为什么不能直接"跟随 release 事件"**：GitHub Actions 的 `on: release` 只对**自己仓库**的 release 生效，对外部仓库只能轮询（`repository_dispatch` 需要上游配合，不现实）。
- **为什么按「tag == 附带 web-ui 版本」过滤 release**：上游在同一个 repo 发多条版本线——`v0.7.x`（web UI + 桌面）、`v1.x`（Android APK）、`hermes-*-runtime`（runtime），而且 Android/runtime 线的 release 也会附带 `hermes-web-ui-*.tar.gz` 资产，光看资产会误选。可靠特征是 web-ui 线 release 的 tag 与它附带的 web-ui 版本号一致（v0.7.20 ↔ hermes-web-ui-0.7.20.tar.gz；Android v1.0.3 附带的是 0.7.18，不一致即排除）。另外上游会主动把 release 从 "latest" 标记移除，`/releases/latest` 也不可靠。
- **幂等跳过**：构建前用 `docker manifest inspect` 检查 GHCR 里是否已有该 tag，有就跳过；删掉 GHCR 里的包版本后，下一轮会自动重建。
- **为什么不用 alpine**：sharp / sherpa-onnx 等原生模块只有 glibc 版（依赖 `libc.so.6`），alpine 的 musl 跑不了，只能用 Debian slim。
- **无 agent 模式**：web UI 启动时检测不到 hermes CLI，会走「Hermes Agent unavailable; skipping profile gateways and agent bridge」的降级路径，管理/聊天界面正常，agent 相关功能不可用。web UI 本身也能自己下载/管理 hermes 运行时，或通过 `HERMES_BIN` 接外部 CLI。

## 注意事项

- **60 天不活跃会停**：GitHub 会把连续 60 天没有任何 commit 的仓库的 scheduled workflow 自动禁用。如果发现不构建了，去 Actions 页面重新 Enable（或随便 push 一个 commit）。
- **默认只构建 amd64**：够用且快。需要 arm64 时手动触发把 `platforms` 填 `linux/amd64,linux/arm64`（arm64 是 QEMU 模拟构建，较慢）。
- **静态 ffmpeg 下载源**：`with_ffmpeg: true` 时从 `github.com/eugeneware/ffmpeg-static` 的 release 下载（与 npm 包 `ffmpeg-static` 同源），GitHub Actions 上网络正常。若该源不可达，构建会失败，此时去掉 ffmpeg 选项即可。
- **许可证**：上游是 BSL-1.1（source-available）。自用/内部使用没问题，但**不要把构建出的镜像公开分发**，GHCR 包保持 private 即可。
- 上游其实官方发布了预构建镜像 `ekkoye8888/hermes-web-ui`（Docker Hub，含 agent），哪天想要完整功能可以直接用它。
