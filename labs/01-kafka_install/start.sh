#!/bin/bash

export THIS_SHELL_PATH="$(readlink -f "$0")"
export THIS_SHELL_DIR="$(dirname "${THIS_SHELL_PATH}")"

docker-compose -f "${THIS_SHELL_DIR}/docker-compose.yml" up -d --build