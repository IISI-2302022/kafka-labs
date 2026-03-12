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

"${container_engine}" network create --driver bridge kafka-labs

"${container_engine}" run -d \
  --name kafka \
  --network kafka-labs \
  -p 29092:29092 \
  --restart always \
  -v "${THIS_SHELL_DIR}/kafka/data:/var/lib/kafka/data" \
  -e KAFKA_NODE_ID=1 \
  -e CLUSTER_ID="MkU3OEVBNTcwNTJENDM2Qk" \
  -e KAFKA_PROCESS_ROLES=controller,broker \
  -e KAFKA_LISTENERS="PLAINTEXT://kafka:9092,CONTROLLER://kafka:9093,PLAINTEXT_HOST://0.0.0.0:29092" \
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT" \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@kafka:9093" \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE="true" \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  -e KAFKA_DELETE_TOPIC_ENABLE="true" \
  confluentinc/cp-kafka:7.9.1

"${container_engine}" run -d \
  --name kafka-ui-demo \
  --network kafka-labs \
  -p 8080:8080 \
  --restart always \
  -v "${THIS_SHELL_DIR}/kafka-ui/data/:/etc/kafkaui/" \
  -e DYNAMIC_CONFIG_ENABLED=true \
  -e DYNAMIC_CONFIG_PATH=/etc/kafkaui/dynamic_config.yaml \
  -e AUTH_TYPE=DISABLED \
  -e LOGGING_LEVEL_ROOT=INFO \
  provectuslabs/kafka-ui:v0.7.2

#"${container_engine}" compose -f "${THIS_SHELL_DIR}/docker-compose.yml" up -d --build