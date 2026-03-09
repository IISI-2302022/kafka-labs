#!/bin/bash

# 基本環境變數
# 停用 MSYS(Git Bash) 自動路徑轉換（詳見 01-kafka_install/start.sh 說明）
export MSYS_NO_PATHCONV=1

export THIS_SHELL_PATH="$(readlink -f "$0")"
export THIS_SHELL_DIR="$(dirname "${THIS_SHELL_PATH}")"

# 依數字由大到小的順序，逐一調用每個 lab 目錄中的 stop.sh
while IFS= read -r LAB_DIR; do
  STOP_SH="${LAB_DIR}/stop.sh"
  if [ -f "${STOP_SH}" ]; then
    echo -e "\n============================= stop ${LAB_DIR} start ============================="
    chmod u+x "${STOP_SH}"
    "${STOP_SH}"
    echo -e "\n============================= stop ${LAB_DIR} end ==============================="
  fi
done < <(find "${THIS_SHELL_DIR}" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' \
         | while IFS= read -r d; do echo "$(basename "$d") $d"; done \
         | sort -rn -k1 \
         | cut -d' ' -f2-)
