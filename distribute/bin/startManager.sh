#!/usr/bin/env bash
SHELL_SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_PATH=${SHELL_SCRIPT_PATH}/../lib
export CLASSPATH=${CLASSPATH}:${LIB_PATH}/*
java -cp ${CLASSPATH}:${LIB_PATH}/lemon-schedule-manager-1.0-SNAPSHOT.jar com.gabry.job.manager.daemon.JobManagerDaemon
