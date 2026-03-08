#!/bin/bash


# 基本環境變數
export SCRIPT_PATH="$(readlink -f "$0")"
export SCRIPT_DIR_PATH="$(dirname "${SCRIPT_PATH}")"

chmod u+x "${SCRIPT_DIR_PATH}/delete_all_obj.sh"
"${SCRIPT_DIR_PATH}/delete_all_obj.sh"

chmod u+x "${SCRIPT_DIR_PATH}/create_all_obj.sh"
"${SCRIPT_DIR_PATH}/create_all_obj.sh"
