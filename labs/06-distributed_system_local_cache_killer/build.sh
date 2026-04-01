#!/bin/bash

export THIS_SHELL_PATH="$(readlink -f "$0")"
export THIS_SHELL_DIR="$(dirname "${THIS_SHELL_PATH}")"

mvn -f "${THIS_SHELL_DIR}/kafka-cache-killer" clean package -DskipTests