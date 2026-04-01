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

mkdir -p "${THIS_SHELL_DIR}/mysql/data"

"${container_engine}" run -d \
  --name mysql \
  --network kafka-labs \
  --restart always \
  -p 3306:3306 \
  -e TZ=Asia/Taipei \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=kafka_source \
  -e MYSQL_USER=ming \
  -e MYSQL_PASSWORD=ming \
  -v "${THIS_SHELL_DIR}/mysql/init-scripts:/docker-entrypoint-initdb.d" \
  -v "${THIS_SHELL_DIR}/mysql/data:/var/lib/mysql" \
  mysql:8.0

mkdir -p "${THIS_SHELL_DIR}/postgres/data"

"${container_engine}" run -d \
  --name postgresql \
  --network kafka-labs \
  --restart always \
  -p 5432:5432 \
  -e POSTGRES_DB=kafka_sink \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin \
  -v "${THIS_SHELL_DIR}/postgres/data:/var/lib/postgresql/data" \
  postgres:13.16


"${container_engine}" run -d \
  --name kafka-connect \
  --network kafka-labs \
  --restart always \
  -p 8083:8083 \
  -v "${THIS_SHELL_DIR}/kafka/connectors/confluentinc-kafka-connect-jdbc-10.8.4/lib/:/usr/share/java/kafka-connect-jdbc/" \
  -e CONNECT_BOOTSTRAP_SERVERS=kafka:9092 \
  -e CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect \
  -e CONNECT_REST_PORT=8083 \
  -e CONNECT_GROUP_ID=kafka-connect \
  -e CONNECT_SESSION_TIMEOUT_MS=10000 \
  -e CONNECT_HEARTBEAT_INTERVAL_MS=3000 \
  -e CONNECT_CONSUMER_METADATA_MAX_AGE_MS=10000 \
  -e CONNECT_CONFIG_STORAGE_TOPIC=connect-configs \
  -e CONNECT_OFFSET_STORAGE_TOPIC=connect-offsets \
  -e CONNECT_STATUS_STORAGE_TOPIC=connect-status \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_INTERNAL_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_INTERNAL_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_PLUGIN_PATH=/usr/share/java/,/usr/share/confluent-hub-components/ \
  confluentinc/cp-kafka-connect:7.9.1

#"${container_engine}" compose -f "${THIS_SHELL_DIR}/docker-compose.yml" up -d --build