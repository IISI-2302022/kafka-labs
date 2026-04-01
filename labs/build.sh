#!/bin/bash

# 基本環境變數
export THIS_SHELL_PATH="$(readlink -f "$0")"
export THIS_SHELL_DIR="$(dirname "${THIS_SHELL_PATH}")"

# 依數字由小到大的順序，逐一調用每個 lab 目錄中的 start.sh
while IFS= read -r LAB_DIR; do
  BUILD_SH="${LAB_DIR}/build.sh"
  if [ -f "${BUILD_SH}" ]; then
    echo -e "\n============================= build ${LAB_DIR} start ============================="
    chmod u+x "${BUILD_SH}"
    "${BUILD_SH}"
    echo -e "\n============================= build ${LAB_DIR} end ==============================="
  fi
done < <(find "${THIS_SHELL_DIR}" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' \
         | while IFS= read -r d; do echo "$(basename "$d") $d"; done \
         | sort -n -k1 \
         | cut -d' ' -f2-)
