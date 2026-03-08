#!/bin/bash

# 基本環境變數
# 停用 MSYS(Git Bash) 自動路徑轉換（詳見 start.sh 說明）
export MSYS_NO_PATHCONV=1

export THIS_SHELL_PATH="$(readlink -f "$0")"
export THIS_SHELL_DIR="$(dirname "${THIS_SHELL_PATH}")"

chmod u+x "${THIS_SHELL_DIR}/stop.sh"
"${THIS_SHELL_DIR}/stop.sh"

rm -rf "${THIS_SHELL_DIR}/kafka/data/"*
rm -rf "${THIS_SHELL_DIR}/kafka-ui/data/"*