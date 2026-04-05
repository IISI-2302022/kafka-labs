#!/bin/bash

# 基本環境變數

export THIS_SHELL_PATH="$(readlink -f "$0")"
export THIS_SHELL_DIR="$(dirname "${THIS_SHELL_PATH}")"

chmod u+x "${THIS_SHELL_DIR}/stop.sh"
"${THIS_SHELL_DIR}/stop.sh"