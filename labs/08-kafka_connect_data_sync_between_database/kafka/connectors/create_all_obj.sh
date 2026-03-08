#!/bin/bash

# 基本環境變數
export SCRIPT_PATH="$(readlink -f "$0")"
export SCRIPT_DIR_PATH="$(dirname "${SCRIPT_PATH}")"

while IFS= read -r CONNECTOR_DIR; do
  OBJECT_DIR="${CONNECTOR_DIR}/obj"
  if [ -d "${OBJECT_DIR}" ] ; then
    while IFS= read -r OBJECT_JSON; do
        echo -e "\n-----------------------------create ${OBJECT_JSON} start-----------------------------------------"
          curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@${OBJECT_JSON}"
        echo -e "\n-----------------------------create ${OBJECT_JSON} end-----------------------------------------"
      done < <(find "${OBJECT_DIR}" -name "*.json" -type f)
  fi
done < <(find "${SCRIPT_DIR_PATH}" -mindepth 1 -maxdepth 1 -type d)

