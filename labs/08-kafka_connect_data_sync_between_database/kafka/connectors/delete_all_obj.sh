#!/bin/bash

# 基本環境變數
export SCRIPT_PATH="$(readlink -f "$0")"
export SCRIPT_DIR_PATH="$(dirname "${SCRIPT_PATH}")"

while IFS= read -r CONNECTOR_DIR; do
  OBJECT_DIR="${CONNECTOR_DIR}/obj"
  if [ -d "${OBJECT_DIR}" ] ; then
    while IFS= read -r OBJECT_JSON; do
        export OBJ_NAME
        if [[ -n "$(command -v jq)" ]]; then
            OBJ_NAME="$(jq -r '.name' "${OBJECT_JSON}")"
        else
            OBJ_NAME="$(basename "${OBJECT_JSON}" ".json")"
        fi
        echo -e "\n-----------------------------delete ${OBJECT_JSON} start-----------------------------------------"
          curl -X DELETE "http://localhost:8083/connectors/${OBJ_NAME}"
        echo -e "\n-----------------------------delete ${OBJECT_JSON} end-----------------------------------------"
      done < <(find "${OBJECT_DIR}" -name "*.json" -type f)
  fi
done < <(find "${SCRIPT_DIR_PATH}" -mindepth 1 -maxdepth 1 -type d)
