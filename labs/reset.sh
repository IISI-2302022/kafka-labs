#!/bin/bash

# 基本環境變數
# 停用 MSYS(Git Bash) 自動路徑轉換（詳見 start.sh 說明）
export MSYS_NO_PATHCONV=1

export THIS_SHELL_PATH="$(readlink -f "$0")"
export THIS_SHELL_DIR="$(dirname "${THIS_SHELL_PATH}")"

# 依數字由大到小的順序，逐一調用每個 lab 目錄中的 reset.sh
while IFS= read -r LAB_DIR; do
  RESET_SH="${LAB_DIR}/reset.sh"
  if [ -f "${RESET_SH}" ]; then
    echo -e "\n============================= reset ${LAB_DIR} start ============================="
    chmod u+x "${RESET_SH}"
    "${RESET_SH}"
    echo -e "\n============================= reset ${LAB_DIR} end ==============================="
  fi
done < <(find "${THIS_SHELL_DIR}" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' \
         | while IFS= read -r d; do echo "$(basename "$d") $d"; done \
         | sort -rn -k1 \
         | cut -d' ' -f2-)
