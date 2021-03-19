#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MAPR_HOME="${MAPR_HOME:-/opt/mapr}"
ROLES_DIR="${MAPR_HOME}/roles"
SPARK_VERSION="$(cat ${MAPR_HOME}/spark/sparkversion)"
SPARK_HOME="${MAPR_HOME}/spark/spark-${SPARK_VERSION}"
SPARK_PID_DIR="${SPARK_HOME}/pid"
SPARK_IDENT_STRING="$USER"
LOG_FILE=${SPARK_HOME}/logs/verify_service

EXIT_SUCCESS=0
EXIT_NOT_RUNNING=1
EXIT_RUNNING_NOT_RESPONDING=2

MASTER_PID_FILE=${SPARK_PID_DIR}/spark-${SPARK_IDENT_STRING}-org.apache.spark.deploy.master.Master-1.pid
HISTORY_PID_FILE=${SPARK_PID_DIR}/spark-${SPARK_IDENT_STRING}-org.apache.spark.deploy.history.HistoryServer-1.pid
THRIFT_PID_FILE=${SPARK_PID_DIR}/spark-${SPARK_IDENT_STRING}-org.apache.spark.sql.hive.thriftserver.HiveThriftServer2-1.pid

MASTER_CONFIGURED_PORT=$(grep -F standalone.port ${SPARK_HOME}/conf/spark-defaults.conf | cut -d ' ' -f 2)
HISTORY_CONFIGURED_PORT=$(grep -F historyServer.port ${SPARK_HOME}/conf/spark-defaults.conf | cut -d ' ' -f 2)
MASTER_PORT="${MASTER_CONFIGURED_PORT:-8080}"
HISTORY_PORT="${HISTORY_CONFIGURED_PORT:-18080}"
THRIFT_PORT=2304

logInfo() {
    message="$1"
    echo "$(timestamp) [INFO] $message" | tee -a "$LOG_FILE"
}

logError() {
    message="$1"
    echo "$(timestamp) [ERROR] $message" | tee -a "$LOG_FILE"
}

timestamp() {
    date +"[%Y-%m-%d %H:%M:%S]" # current time
}

is_responding() {
    DAEMON=$1
    PORT=$2
    logInfo "Checking if $DAEMON port $PORT is open"
    nohup nc localhost "$PORT" </dev/null >/dev/null 2>&1
    if [ "$?" == 0 ]; then
        logInfo "$DAEMON port $PORT is open"
        return $EXIT_SUCCESS
    else
        logError "$DAEMON port $PORT is not open"
        return $EXIT_RUNNING_NOT_RESPONDING
    fi
}

is_running() {
    SERVICE="$1"
    PID_FILE="$2"
    logInfo "Starting $SERVICE verifier at $(timestamp)"
    if [ -e "$PID_FILE" ] || [ -h "$PID_FILE" ]; then
        process_pid=$(cat "$PID_FILE" 2>/dev/null)
        if [ $? -ne 0 ]; then
            PID_FILE=$(ls -l "$PID_FILE" | awk '{print $11}')
            process_pid=$(cat "$PID_FILE" 2>/dev/null)
        fi
        if [ -z "$process_pid" ]; then
            logError "ERROR - could not get pid for $SERVICE"
            return $EXIT_NOT_RUNNING
        fi
        logInfo "checking to see if pid $process_pid is alive"
        if kill -s 0 "$process_pid" 2>/dev/null; then
            logInfo "pid $process_pid is alive"
            return $EXIT_SUCCESS
        else
            logInfo "pid $process_pid is NOT running"
            return $EXIT_NOT_RUNNING
        fi
    else
        logInfo "no pid file, service $SERVICE is NOT running"
        return $EXIT_NOT_RUNNING
    fi
}


logInfo "Starting verifier at $(date)"

MASTER_STATUS="$EXIT_SUCCESS"
if [ -f "$ROLES_DIR/spark-master" ]; then
    logInfo "Spark Master is installed"
    if is_running master "$MASTER_PID_FILE"; then
        if is_responding master "$MASTER_PORT"; then
            MASTER_STATUS="$EXIT_SUCCESS"
            logInfo "Spark Master is responsive"
        else
            MASTER_STATUS="$EXIT_RUNNING_NOT_RESPONDING"
            logInfo "Spark Master is running but not responsive"
        fi
    else
        MASTER_STATUS="$EXIT_NOT_RUNNING"
        logInfo "Spark Master is not running"
    fi
else
    logInfo "Spark Master is not installed"
fi

HISTORY_STATUS="$EXIT_SUCCESS"
if [ -f "$ROLES_DIR/spark-historyserver" ]; then
    logInfo "Spark history server is installed"
    if is_running historyserver "$HISTORY_PID_FILE"; then
        if is_responding historyserver "$HISTORY_PORT"; then
            HISTORY_STATUS="$EXIT_SUCCESS"
            logInfo "Spark history server is responsive"
        else
            HISTORY_STATUS="$EXIT_RUNNING_NOT_RESPONDING"
            logInfo "Spark history server is running but not responsive"
        fi
    else
        HISTORY_STATUS="$EXIT_NOT_RUNNING"
        logInfo "Spark history server is not running"
    fi
else
    logInfo "Spark history server is not installed"
fi

THRIFT_STATUS="$EXIT_SUCCESS"
if [ -f "$ROLES_DIR/spark-thriftserver" ]; then
    logInfo "Thriftserver is installed"
    if is_running thriftserver "$THRIFT_PID_FILE"; then
        if is_responding thriftserver $THRIFT_PORT; then
            THRIFT_STATUS="$EXIT_SUCCESS"
            logInfo "Thriftserver is responsive"
        else
            THRIFT_STATUS="$EXIT_RUNNING_NOT_RESPONDING"
            logInfo "Thriftserver is running but not responsive"
        fi
    else
        THRIFT_STATUS="$EXIT_NOT_RUNNING"
        logInfo "Thriftserver is not running"
    fi
else
    logInfo "Thriftserver is not installed"
fi


if [ "$THRIFT_STATUS" = "$EXIT_SUCCESS" ] && [ "$HISTORY_STATUS" = "$EXIT_SUCCESS" ] && [ "$MASTER_STATUS" = "$EXIT_SUCCESS" ]; then
    exit "$EXIT_SUCCESS"
fi

if [ "$THRIFT_STATUS" = "$EXIT_RUNNING_NOT_RESPONDING" ] || [ "$HISTORY_STATUS" = "$EXIT_RUNNING_NOT_RESPONDING" ] || [ "$MASTER_STATUS" = "$EXIT_RUNNING_NOT_RESPONDING" ] ; then
    exit "$EXIT_RUNNING_NOT_RESPONDING"
fi

exit "$EXIT_NOT_RUNNING"
