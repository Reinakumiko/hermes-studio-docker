# hermes-studio ↔ hermes gateway bridge 镜像
# 用法：docker run ... bridge.py --gateway ws://<hermes主机>:9119/api/ws [--token X]
FROM python:3.12-slim

RUN pip install --no-cache-dir "websockets>=10,<14"

WORKDIR /app
COPY bridge.py .

RUN useradd --create-home --home-dir /home/bridge --shell /bin/bash bridge \
    && mkdir -p /data && chown bridge:bridge /data

USER bridge
ENV STATE_FILE=/data/bridge_state.json
EXPOSE 18765

ENTRYPOINT ["python3", "/app/bridge.py"]
CMD ["--listen", "0.0.0.0:18765", "--state-file", "/data/bridge_state.json"]