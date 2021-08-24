#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This file is sourced when running various Spark programs.
# Copy it as spark-env.sh and edit that to configure Spark for your site.

# Options read when launching programs locally with
# ./bin/run-example or ./bin/spark-submit
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - SPARK_PUBLIC_DNS, to set the public dns name of the driver program

# Options read by executors and drivers running inside the cluster
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - SPARK_PUBLIC_DNS, to set the public DNS name of the driver program
# - SPARK_LOCAL_DIRS, storage directories to use on this node for shuffle and RDD data
# - MESOS_NATIVE_JAVA_LIBRARY, to point to your libmesos.so if you use Mesos

# Options read in YARN client/cluster mode
# - SPARK_CONF_DIR, Alternate conf dir. (Default: ${SPARK_HOME}/conf)
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
# - YARN_CONF_DIR, to point Spark towards YARN configuration files when you use YARN
# - SPARK_EXECUTOR_CORES, Number of cores for the executors (Default: 1).
# - SPARK_EXECUTOR_MEMORY, Memory per Executor (e.g. 1000M, 2G) (Default: 1G)
# - SPARK_DRIVER_MEMORY, Memory for Driver (e.g. 1000M, 2G) (Default: 1G)

# Options for the daemons used in the standalone deploy mode
# - SPARK_MASTER_HOST, to bind the master to a different IP address or hostname
# - SPARK_MASTER_PORT / SPARK_MASTER_WEBUI_PORT, to use non-default ports for the master
# - SPARK_MASTER_OPTS, to set config properties only for the master (e.g. "-Dx=y")
# - SPARK_WORKER_CORES, to set the number of cores to use on this machine
# - SPARK_WORKER_MEMORY, to set how much total memory workers have to give executors (e.g. 1000m, 2g)
# - SPARK_WORKER_PORT / SPARK_WORKER_WEBUI_PORT, to use non-default ports for the worker
# - SPARK_WORKER_DIR, to set the working directory of worker processes
# - SPARK_WORKER_OPTS, to set config properties only for the worker (e.g. "-Dx=y")
# - SPARK_DAEMON_MEMORY, to allocate to the master, worker and history server themselves (default: 1g).
# - SPARK_HISTORY_OPTS, to set config properties only for the history server (e.g. "-Dx=y")
# - SPARK_SHUFFLE_OPTS, to set config properties only for the external shuffle service (e.g. "-Dx=y")
# - SPARK_DAEMON_JAVA_OPTS, to set config properties for all daemons (e.g. "-Dx=y")
# - SPARK_DAEMON_CLASSPATH, to set the classpath for all daemons
# - SPARK_PUBLIC_DNS, to set the public dns name of the master or workers

# Options for launcher
# - SPARK_LAUNCHER_OPTS, to set config properties and Java options for the launcher (e.g. "-Dx=y")

# Generic options for the daemons used in the standalone deploy mode
# - SPARK_CONF_DIR      Alternate conf dir. (Default: ${SPARK_HOME}/conf)
# - SPARK_LOG_DIR       Where log files are stored.  (Default: ${SPARK_HOME}/logs)
# - SPARK_LOG_MAX_FILES Max log files of Spark daemons can rotate to. Default is 5.
# - SPARK_PID_DIR       Where the pid file is stored. (Default: /tmp)
# - SPARK_IDENT_STRING  A string representing this instance of spark. (Default: $USER)
# - SPARK_NICENESS      The scheduling priority for daemons. (Default: 0)
# - SPARK_NO_DAEMONIZE  Run the proposed command in the foreground. It will not output a PID file.
# Options for native BLAS, like Intel MKL, OpenBLAS, and so on.
# You might get better performance to enable these options if using native BLAS (see SPARK-21305).
# - MKL_NUM_THREADS=1        Disable multi-threading of Intel MKL
# - OPENBLAS_NUM_THREADS=1   Disable multi-threading of OpenBLAS

#########################################################################################################
# Set MapR attributes and compute classpath
#########################################################################################################

MAPR_HOME="${MAPR_HOME:-/opt/mapr}"
SPARK_VERSION=$(cat ${MAPR_HOME}/spark/sparkversion)
SPARK_HOME=${MAPR_HOME}/spark/spark-${SPARK_VERSION}

# Set the spark attributes
if [ -d "${SPARK_HOME}" ]; then
  export SPARK_HOME=${SPARK_HOME}
fi

# Load the hadoop version attributes
source ${SPARK_HOME}/mapr-util/hadoop-version-picker.sh
export HADOOP_HOME=$hadoop_home_dir
export HADOOP_CONF_DIR=$hadoop_conf_dir

# Enable mapr impersonation
export MAPR_IMPERSONATION_ENABLED=1

MAPR_HADOOP_CLASSPATH=`${SPARK_HOME}/bin/mapr-classpath.sh`
MAPR_HADOOP_JNI_PATH=`hadoop jnipath`
MAPR_SPARK_CLASSPATH="$MAPR_HADOOP_CLASSPATH"

#FIX for SPARK-906/KAFKA-717. This should be removed when kafka-eventstreams jar will be added to mapr classpath
if [ -f "$MAPR_HOME/kafka/kafkaversion" ]; then
        MAPR_SPARK_CLASSPATH=$MAPR_SPARK_CLASSPATH:$(find ${BASEMAPR:-$MAPR_HOME}/kafka -name "kafka-eventstreams-*.jar")
fi

SPARK_MAPR_HOME=$MAPR_HOME

export SPARK_LIBRARY_PATH=$MAPR_HADOOP_JNI_PATH
export LD_LIBRARY_PATH="$MAPR_HADOOP_JNI_PATH:$LD_LIBRARY_PATH"

# Load the classpath generator script
source ${SPARK_HOME}/mapr-util/generate-classpath.sh

# Calculate hive jars to include in classpath
generate_compatible_classpath "spark" "${SPARK_VERSION}" "hive"
MAPR_HIVE_CLASSPATH=${generated_classpath}
if [ ! -z "$MAPR_HIVE_CLASSPATH" ]; then
  MAPR_SPARK_CLASSPATH="$MAPR_SPARK_CLASSPATH:$MAPR_HIVE_CLASSPATH"
fi

# Calculate hbase jars to include in classpath
generate_compatible_classpath "spark" "${SPARK_VERSION}" "hbase"
MAPR_HBASE_CLASSPATH=${generated_classpath}
if [ ! -z "$MAPR_HBASE_CLASSPATH" ]; then
  MAPR_SPARK_CLASSPATH="$MAPR_SPARK_CLASSPATH:$MAPR_HBASE_CLASSPATH"
  SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dspark.driver.extraClassPath=$MAPR_HBASE_CLASSPATH"
fi

# Set executor classpath for MESOS. Uncomment following string if you want deploy spark jobs on Mesos
#MAPR_MESOS_CLASSPATH=$MAPR_SPARK_CLASSPATH
SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dspark.executor.extraClassPath=$MAPR_HBASE_CLASSPATH:$MAPR_MESOS_CLASSPATH"

# Set SPARK_DIST_CLASSPATH
export SPARK_DIST_CLASSPATH=$MAPR_SPARK_CLASSPATH

# Security status
source $MAPR_HOME/conf/env.sh
if [ "$MAPR_SECURITY_STATUS" = "true" ]; then
  SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dhadoop.login=hybrid -Dmapr_sec_enabled=true -Djavax.security.auth.useSubjectCredsOnly=false"
fi

# scala
export SCALA_VERSION=2.12
export SPARK_SCALA_VERSION=$SCALA_VERSION
export SCALA_HOME=${SPARK_HOME}/scala
export SCALA_LIBRARY_PATH=${SCALA_HOME}/lib

# Use a fixed identifier for pid files
export SPARK_IDENT_STRING="mapr"

#########################################################################################################
#    :::CAUTION::: DO NOT EDIT ANYTHING ON OR ABOVE THIS LINE
#########################################################################################################


#
# MASTER HA SETTINGS
#
#export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER  -Dspark.deploy.zookeeper.url=<zookeerper1:5181,zookeeper2:5181,..> -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf -Dzookeeper.sasl.client=false"


# MEMORY SETTINGS
export SPARK_DAEMON_MEMORY=1g
export SPARK_WORKER_MEMORY=16g

# Worker Directory
export SPARK_WORKER_DIR=$SPARK_HOME/tmp

# Environment variable for printing spark command everytime you run spark.Set to "1" to print.
# export SPARK_PRINT_LAUNCH_COMMAND=1

#UI
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Djava.library.path=$SPARK_MAPR_HOME/lib"
