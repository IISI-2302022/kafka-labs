#!/bin/bash

# 基本環境變數

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

chmod u+x "${THIS_SHELL_DIR}/stop.sh"
"${THIS_SHELL_DIR}/stop.sh"

"${container_engine}" volume rm -f mysql-data
"${container_engine}" volume rm -f postgres-data
