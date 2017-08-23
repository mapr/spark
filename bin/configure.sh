#!/usr/bin/env bash

#############################################################################
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
#############################################################################
#
# Configure script for Spark
#
# This script is normally run by the core configure.sh to setup Spark during
# install. If it is run standalone, need to correctly initialize the
# variables that it normally inherits from the master configure.sh
#
#############################################################################
# Import functions and variables from 'common-ecosystem.sh'
#############################################################################
#
# Result codes
#

RETURN_SUCCESS=0
RETURN_ERR_MAPR_HOME=1
RETURN_ERR_ARGS=2
RETURN_ERR_MAPRCLUSTER=3
RETURN_ERR_OTHER=4

#
# Globals
#

MAPR_HOME="${MAPR_HOME:-/opt/mapr}"
. ${MAPR_HOME}/server/common-ecosystem.sh 2> /dev/null # prevent verbose output, set by 'set -x'
if [ $? -ne 0 ]; then
  echo 'Error: Seems that MAPR_HOME is not correctly set or mapr-core is not installed.'
  exit 1
fi 2> /dev/null
{ set +x; } 2>/dev/null

initCfgEnv

MAPR_CONF_DIR=${MAPR_CONF_DIR:-"$MAPR_HOME/conf"}
SPARK_VERSION="2.1.0"
SPARK_HOME="$MAPR_HOME"/spark/spark-"$SPARK_VERSION"
SPARK_BIN="$SPARK_HOME"/bin
SPARK_LOGS="$SPARK_HOME"/logs
DAEMON_CONF=${MAPR_HOME}/conf/daemon.conf

if [ -f $MAPR_HOME/MapRBuildVersion ]; then
  MAPR_MIN_VERSION=4.0
  MAPR_VERSION=`cat $MAPR_HOME/MapRBuildVersion | awk -F "." '{print $1"."$2}'`

  #
  # If the MapR release >=4.0 (yarn beta) returns boolean 1, else returns boolean 0
  #
  if [ $(echo | awk -v cur=$MAPR_VERSION -v min=$MAPR_MIN_VERSION '{if (cur >= min) printf("1"); else printf ("0");}') -eq 0 ]; then
    rm -f "$SPARK_HOME"/lib/spark*hadoop2*.jar
  else
    rm -f "$SPARK_HOME"/lib/spark*hadoop1*.jar
  fi
fi

ln -sfn "$SPARK_HOME" /usr/local/spark

#
# Make the logs directory rwx, and set the sticky bit.
#
mkdir -p "$SPARK_HOME/logs"
chmod  a+rwxt "$SPARK_HOME"/logs

#
# create tmp directory with rwx, and set the sticky bit.
#
mkdir -p "$SPARK_HOME/tmp"
chmod  a+rwxt "$SPARK_HOME"/tmp

#
# Improved default logging level (WARN instead of INFO)
#
sed 's/rootCategory=INFO/rootCategory=WARN/' "$SPARK_HOME/conf/log4j.properties.template" > "$SPARK_HOME/conf/log4j.properties"

#
# Add MapR customization to spark
#
if [ ! -e "$SPARK_HOME"/conf/spark-env.sh ]; then
	cp "$SPARK_HOME"/conf/spark-env.sh.template "$SPARK_HOME"/conf/spark-env.sh
fi
       cat >> "$SPARK_HOME"/conf/spark-env.sh << EOM


#########################################################################################################
# Set MapR attributes and compute classpath
#########################################################################################################

# Set the spark attributes
if [ -d "$SPARK_HOME" ]; then
  export SPARK_HOME=$SPARK_HOME
fi

# Load the hadoop version attributes
source $SPARK_HOME/mapr-util/hadoop-version-picker.sh
export HADOOP_HOME=\$hadoop_home_dir
export HADOOP_CONF_DIR=\$hadoop_conf_dir

# Enable mapr impersonation
export MAPR_IMPERSONATION_ENABLED=1

MAPR_HADOOP_CLASSPATH=\`mapr classpath\`
MAPR_HADOOP_JNI_PATH=\`hadoop jnipath\`
MAPR_SPARK_CLASSPATH="\$MAPR_HADOOP_CLASSPATH"

SPARK_MAPR_HOME=$MAPR_HOME

export SPARK_LIBRARY_PATH=\$MAPR_HADOOP_JNI_PATH
export LD_LIBRARY_PATH="\$MAPR_HADOOP_JNI_PATH:\$LD_LIBRARY_PATH"

# Load the classpath generator script
source $SPARK_HOME/mapr-util/generate-classpath.sh

# Calculate hive jars to include in classpath
generate_compatible_classpath "spark" "$SPARK_VERSION" "hive"
MAPR_HIVE_CLASSPATH=\${generated_classpath}
if [ ! -z "\$MAPR_HIVE_CLASSPATH" ]; then
  MAPR_SPARK_CLASSPATH="\$MAPR_SPARK_CLASSPATH:\$MAPR_HIVE_CLASSPATH"
fi

# Calculate hbase jars to include in classpath
generate_compatible_classpath "spark" "$SPARK_VERSION" "hbase"
MAPR_HBASE_CLASSPATH=\${generated_classpath}
if [ ! -z "\$MAPR_HBASE_CLASSPATH" ]; then
  MAPR_SPARK_CLASSPATH="\$MAPR_SPARK_CLASSPATH:\$MAPR_HBASE_CLASSPATH"
  SPARK_SUBMIT_OPTS="\$SPARK_SUBMIT_OPTS -Dspark.driver.extraClassPath=\$MAPR_HBASE_CLASSPATH"
fi

# Set executor classpath for MESOS. Uncomment following string if you want deploy spark jobs on Mesos
#MAPR_MESOS_CLASSPATH=\$MAPR_SPARK_CLASSPATH
SPARK_SUBMIT_OPTS="\$SPARK_SUBMIT_OPTS -Dspark.executor.extraClassPath=\$MAPR_HBASE_CLASSPATH:\$MAPR_MESOS_CLASSPATH"

# Set SPARK_DIST_CLASSPATH
export SPARK_DIST_CLASSPATH=\$MAPR_SPARK_CLASSPATH

# Security status
source $MAPR_HOME/conf/env.sh
if [ "\$MAPR_SECURITY_STATUS" = "true" ]; then
  SPARK_SUBMIT_OPTS="\$SPARK_SUBMIT_OPTS -Dhadoop.login=hybrid -Dmapr_sec_enabled=true"
fi

# scala
export SCALA_VERSION=2.11
export SPARK_SCALA_VERSION=\$SCALA_VERSION
export SCALA_HOME=$SPARK_HOME/scala
export SCALA_LIBRARY_PATH=\$SCALA_HOME/lib

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
export SPARK_WORKER_DIR=\$SPARK_HOME/tmp

# Environment variable for printing spark command everytime you run spark.Set to "1" to print.
# export SPARK_PRINT_LAUNCH_COMMAND=1


EOM

source $MAPR_HOME/conf/env.sh
        if [ "$MAPR_SECURITY_STATUS" = "true" ]; then
          sed -i '/# Security/,/# EndOfSecurityConfiguration/d' "$SPARK_HOME"/conf/spark-defaults.conf
          cat >> "$SPARK_HOME"/conf/spark-defaults.conf << EOM

# Security
# - ACLS
spark.acls.enable       true
spark.admin.acls        mapr
spark.admin.acls.groups mapr
# - Authorization and Network Encryption
spark.authenticate      true
# - - This secret will be used only by local/standalone modes. YARN will override this with its own secret
spark.authenticate.secret       changeMe
spark.authenticate.enableSaslEncryption true
spark.network.sasl.serverAlwaysEncrypt  true
# - IO Encryption
spark.io.encryption.enabled     true
spark.io.encryption.keySizeBits 128
# EndOfSecurityConfiguration
EOM
        fi

        #
        # If the spark version is greater than 1.0, remove
        # the any shark directory that is left over.  Otherwise,
        # add MapR customization of shark.
        #
        SPARK_MIN_VERSION=1.0
        if [ $(echo | awk -v cur=$SPARK_VERSION -v min=$SPARK_MIN_VERSION '{if (cur >= min) printf("1"); else printf ("0");}') -eq 0 ]; then
             MAPR_SHARK_HOME=$(dir -d -1 $MAPR_HOME/shark/* 2> /dev/null | head -1)
             cp $MAPR_SHARK_HOME/conf/shark-env.sh.template $MAPR_SHARK_HOME/conf/shark-env.sh

             cat >> $MAPR_SHARK_HOME/conf/shark-env.sh << EOM

# Load the hadoop version attributes
source $SPARK_HOME/mapr-util/hadoop-version-picker.sh
export HADOOP_HOME=\$hadoop_home_dir
#export MASTER=SET MASTER URL(eg: spark://master-hostname:7077)
export SPARK_HOME=$SPARK_HOME
#export HIVE_CONF_DIR=

source \$SPARK_HOME/conf/spark-env.sh

EOM
        else
             rm -rf $MAPR_HOME/shark
        fi

#####################################
#     Functions warden/permission
#####################################

#
# Change permission
#

function change_permissions() {
    if [ -f $DAEMON_CONF ]; then
        MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
        MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)

        if [ ! -z "$MAPR_USER" ]; then
            chown -R ${MAPR_USER} ${SPARK_HOME}
        fi

	    if [ ! -z "$MAPR_GROUP" ]; then
            chgrp -R ${MAPR_GROUP} ${SPARK_HOME}
        fi
        chmod -f u+x $SPARK_HOME/bin/*
    fi
}

#
# Add warden files
#

function installWardenConfFile() {
    if checkNetworkPortAvailability 8080 2>/dev/null; then
    	{ set +x; } 2>/dev/null
        #Register port for spark master
        registerNetworkPort spark_master 8080 2>/dev/null

        cp "${SPARK_HOME}/warden/warden.spark-master.conf" "${MAPR_CONF_DIR}/conf.d/" 2>/dev/null || :
        logInfo 'Warden conf for Spark-master copied.'
    else
    	{ set +x; } 2>/dev/null
        logErr 'Spark-master  cannot start because its ports already has been taken.'
        exit $RETURN_ERR_MAPRCLUSTER
    fi

    if checkNetworkPortAvailability 18080 2>/dev/null; then
        #Register port for spark historyserver
        { set +x; } 2>/dev/null
        registerNetworkPort spark_historyserver 18080 2>/dev/null

        cp "${SPARK_HOME}/warden/warden.spark-historyserver.conf" "${MAPR_CONF_DIR}/conf.d/" 2>/dev/null || :
        logInfo 'Warden conf for Spark-historyserver copied.'
    else
    	{ set +x; } 2>/dev/null
        logErr 'Spark-historyserver  cannot start because its ports already has been taken.'
        exit $RETURN_ERR_MAPRCLUSTER
    fi


    if checkNetworkPortAvailability 4040 2>/dev/null; then
        #Register port for spark thriftserver
        { set +x; } 2>/dev/null
        registerNetworkPort spark_thriftserver 4040 2>/dev/null

        cp "${SPARK_HOME}/warden/warden.spark-thriftserver.conf" "${MAPR_CONF_DIR}/conf.d/" 2>/dev/null || :
        logInfo 'Warden conf for Spark-thriftserver copied.'
    else
    	{ set +x; } 2>/dev/null
        logErr 'Spark-thriftserver  cannot start because its ports already has been taken.'
        exit $RETURN_ERR_MAPRCLUSTER
    fi
}

function stopServicesForRestartByWarden() {
	#Stop spark master
	if [ -e ${MAPR_CONF_DIR}/conf.d/warden.spark-master.conf ]; then
		${SPARK_HOME}/sbin/stop-master.sh
	fi

	#Stop spark historyserver
	if [ -e ${MAPR_CONF_DIR}/conf.d/warden.spark-historyserver.conf ]; then
		${SPARK_HOME}/sbin/stop-history-server.sh
	fi

	#Stop spark thriftserver
	if [ -e ${MAPR_CONF_DIR}/conf.d/warden.spark-thriftserver.conf ]; then
		${SPARK_HOME}/sbin/stop-thriftserver.sh
	fi
}
#
# Parse options
#

USAGE="usage: $0 [-h] [-R] [-secure] [-unsecure]"

OPTS=`getopt -n "$0" -a -o h -l R -l EC: -l secure -l unsecure -- "$@"`

if [ $? != 0 ] || [ ${#} -lt 1 ] ; then
  echo "${USAGE}"
  exit $RETURN_ERR_ARGS
fi

eval set -- "$OPTS"

for i ; do
  case "$i" in
    --secure)
      isSecure=1;
      shift 1;;
    --unsecure)
      isSecure=0;
      shift 1;;
    --help)
      echo "${USAGE}"
      exit $RETURN_SUCCESS
      ;;
    --)
      shift;;
    *)
      # Invalid arguments passed
      echo "${USAGE}"
      exit $RETURN_ERR_ARGS
  esac
done

change_permissions
installWardenConfFile
stopServicesForRestartByWarden

exit $RETURN_SUCCESS
