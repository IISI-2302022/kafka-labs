#!/bin/bash

# ============================================================
# 停用 MSYS(Git Bash) 自動路徑轉換
# ============================================================
# 在 Windows Git Bash (MSYS2/MinGW) 中，MSYS 會自動將「看起來像 POSIX 絕對路徑」的字串
# 轉換為 Windows 路徑，例如：
#   /d/project/kafka/data:/var/lib/kafka/data
#   → D:/project/kafka/data;C:/var/lib/kafka/data
#
# 冒號 ":" 被解讀為路徑分隔符，後面的 /var 被轉成 C:/var，
# 導致 docker -v 掛載的 volume 路徑變成 "...data;C/var/lib/kafka/data"，
# 進而在本機產生名為 ";C" 的異常目錄。
#
# 設定 MSYS_NO_PATHCONV=1 可完全關閉此自動轉換行為。
# （此變數僅對 MSYS/Git Bash 有效，在 Linux/macOS 上不會造成影響）
# ============================================================
export MSYS_NO_PATHCONV=1

export THIS_SHELL_PATH="$(readlink -f "$0")"
export THIS_SHELL_DIR="$(dirname "${THIS_SHELL_PATH}")"

if type podman &> /dev/null; then
  export container_engine=podman
elif type docker &> /dev/null; then
  export container_engine=docker
else
  echo "Error: No container engine found. Please install Podman or Docker." >&2
  exit 1
fi

"${container_engine}" run -d \
  --name kafka-rest-proxy \
  --network kafka-labs \
  --restart always \
  -p 8082:8082 \
  -e KAFKA_REST_HOST_NAME=kafka-rest-proxy \
  -e KAFKA_REST_BOOTSTRAP_SERVERS='kafka:9092' \
  -e KAFKA_REST_LISTENERS="http://0.0.0.0:8082" \
  confluentinc/cp-kafka-rest:7.9.1

#"${container_engine}" compose -f "${THIS_SHELL_DIR}/docker-compose.yml" up -d --build