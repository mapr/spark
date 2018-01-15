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
HIVE_VERSION=`cat $MAPR_HOME/hive/hiveversion`
SPARK_HOME="$MAPR_HOME"/spark/spark-"$SPARK_VERSION"
HIVE_HOME="$MAPR_HOME"/hive/hive-"$HIVE_VERSION"
SPARK_BIN="$SPARK_HOME"/bin
SPARK_LOGS="$SPARK_HOME"/logs
DAEMON_CONF=${MAPR_HOME}/conf/daemon.conf

CLUSTER_INFO=`cat $MAPR_HOME/conf/mapr-clusters.conf`

IS_FIRST_RUN=false
if [ -f $SPARK_HOME/etc/.not_configured_yet ] ; then
	IS_FIRST_RUN=true
fi

# Spark ports
sparkHSUIPort=18080
isSparkHSUIPortDef=false
sparkTSPort=2304
isSparkTSPortDef=false
sparkTSUIPort=4040
isSparkTSUIPortDef=false
sparkMasterPort=7077
isSparkMasterPortDef=false
sparkMasterUIPort=8080
isSparkMasterUIPortDef=false

# secure ui ports
sparkMasterSecureUIPort=8480
sparkHSSecureUIPort=18480

# indicates whether cluster is up or not
SPARK_IS_RUNNING=false
if [ ! -z ${isOnlyRoles+x} ]; then # isOnlyRoles exists
	if [ $isOnlyRoles -eq 1 ] ; then
        SPARK_IS_RUNNING=true;
    fi
fi

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
# Change config functions
#

function changeSparkDefaults() {
	if grep -q $1 "$SPARK_HOME/conf/spark-defaults.conf"; then
		sed -i "s~^$1.*~$2~" $SPARK_HOME/conf/spark-defaults.conf
	else
		cat >> "$SPARK_HOME"/conf/spark-defaults.conf << EOM
$2
EOM
	fi
}

function changeWardenConfig() {
	if [ -f $SPARK_HOME/warden/warden.spark-$3.conf ] ; then
		sed -i "s~^$1.*~$2~" $SPARK_HOME/warden/warden.spark-$3.conf
	fi
}

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
# Configure security
#

function configureSecurity() {
if [ -f $SPARK_HOME/warden/warden.spark-master.conf ] ; then
	changeWardenConfig "service.ui.port" "service.ui.port=$sparkMasterUIPort" "master"
fi
if [ -f $SPARK_HOME/warden/warden.spark-thriftserver.conf ] ; then
	changeWardenConfig "service.ui.port" "service.ui.port=$sparkTSUIPort" "thriftserver"
fi
if [ -f $SPARK_HOME/warden/warden.spark-historyserver.conf ] ; then
	changeWardenConfig "service.ui.port" "service.ui.port=$sparkHSUIPort" "historyserver"
fi
sed -i '/# SECURITY BLOCK/,/# END OF THE SECURITY CONFIGURATION BLOCK/d' "$SPARK_HOME"/conf/spark-defaults.conf
if [ "$isSecure" == 1 ] ; then
	source $MAPR_HOME/conf/env.sh
    cat >> "$SPARK_HOME"/conf/spark-defaults.conf << EOM
# SECURITY BLOCK
# ALL SECURITY PROPERTIES MUST BE PLACED IN THIS BLOCK

# ssl
spark.ssl.enabled true
spark.ssl.ui.enabled false
spark.ssl.fs.enabled true
spark.ssl.keyPassword mapr123
spark.ssl.trustStore $MAPR_HOME/conf/ssl_truststore
spark.ssl.trustStorePassword mapr123
spark.ssl.keyStore $MAPR_HOME/conf/ssl_keystore
spark.ssl.keyStorePassword mapr123
spark.ssl.protocol TLSv1.2
spark.ssl.enabledAlgorithms TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA

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
# END OF THE SECURITY CONFIGURATION BLOCK

EOM
	if [ -f $SPARK_HOME/warden/warden.spark-master.conf ] ; then
		changeWardenConfig "service.ui.port" "service.ui.port=$sparkMasterSecureUIPort" "master"
	fi
	if [ -f $SPARK_HOME/warden/warden.spark-historyserver.conf ] ; then
		changeWardenConfig "service.ui.port" "service.ui.port=$sparkHSSecureUIPort" "historyserver"
	fi
	case "$CLUSTER_INFO" in
		*"secure=true"*)
		if [ ! -f $SPARK_HOME/conf/hive-site.xml ] ; then
				cp $SPARK_HOME/conf/hive-site.xml.security.template $SPARK_HOME/conf/hive-site.xml
			else
				if ! grep -q hive.server2.thrift.sasl.qop "$SPARK_HOME/conf/hive-site.xml"; then
					CONF="</configuration>"
					PROPERTIES="<property>\n<name>hive.server2.thrift.sasl.qop</name>\n<value>auth-conf</value>\n</property>\n</configuration>"
					sed -i "s~$CONF~$PROPERTIES~g" $SPARK_HOME/conf/hive-site.xml
				fi

				if ! grep -q hive.server2.authentication "$SPARK_HOME/conf/hive-site.xml"; then
					CONF="</configuration>"
					PROPERTIES="<property>\n<name>hive.server2.authentication</name>\n<value>MAPRSASL</value>\n</property>\n</configuration>"
					sed -i "s~$CONF~$PROPERTIES~g" $SPARK_HOME/conf/hive-site.xml
				fi
			fi
		;;
	esac
fi
}

#
# Configure on Hive
#

function configureOnHive() {
	rm -f $SPARK_HOME/conf/hive-site.xml.old
	if [ -f $SPARK_HOME/conf/hive-site.xml ] ; then
		mv $SPARK_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml.old
	fi
	if [ -f $HIVE_HOME/conf/hive-site.xml ] ; then
		cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml
	fi
	if [ -f $SPARK_HOME/conf/hive-site.xml ] ; then
		TEZ_PROP_VALUE="<value>tez</value>"
		MR_PROP_VALUE="<value>mr</value>"
		sed -i "s~$TEZ_PROP_VALUE~$MR_PROP_VALUE~g" $SPARK_HOME/conf/hive-site.xml
	fi
}

#
# Reserve ports
#

function registerPortMaster() {
	if [ -f $SPARK_HOME/warden/warden.spark-master.conf ] ; then
		if checkNetworkPortAvailability $sparkMasterUIPort 2>/dev/null; then
			{ set +x; } 2>/dev/null
			registerNetworkPort spark_master_ui $sparkMasterUIPort 2>/dev/null
		else
			{ set +x; } 2>/dev/null
			logWarn "Spark-master-ui port already has been taken by $(whoHasNetworkPort $sparkMasterUIPort)"
		fi
		if checkNetworkPortAvailability $sparkMasterPort 2>/dev/null; then
			{ set +x; } 2>/dev/null
			registerNetworkPort spark_master $sparkMasterPort 2>/dev/null
		else
			{ set +x; } 2>/dev/null
			logWarn "Spark-master port already has been taken by $(whoHasNetworkPort $sparkMasterPort)"
		fi

		if [ "$isSparkMasterPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] || ! grep -q "spark.master " "$SPARK_HOME/conf/spark-defaults.conf"; then
			changeSparkDefaults "spark.master " "spark.master   spark://$(hostname --fqdn):$sparkMasterPort"
		fi

		if [ "$isSparkMasterPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] ; then
			changeWardenConfig "service.port" "service.port=$sparkMasterPort" "master"
		fi

		if [ "$isSparkMasterUIPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] || ! grep -q "spark.master.ui.port" "$SPARK_HOME/conf/spark-defaults.conf"; then
			changeSparkDefaults "spark.master.ui.port" "spark.master.ui.port	$sparkMasterUIPort"
		fi

		if [ "$isSparkMasterUIPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] ; then
			changeWardenConfig "service.ui.port" "service.ui.port=$sparkMasterUIPort" "master"
		fi
	fi
}

function registerPortThriftServer() {
	if [ -f $SPARK_HOME/warden/warden.spark-thriftserver.conf ] ; then
		if checkNetworkPortAvailability $sparkTSUIPort 2>/dev/null; then
			{ set +x; } 2>/dev/null
			registerNetworkPort spark_thriftserver_ui $sparkTSUIPort 2>/dev/null
		else
			{ set +x; } 2>/dev/null
			logWarn "Spark-thriftServer-ui port already has been taken by $(whoHasNetworkPort $sparkTSUIPort)"
		fi
		if checkNetworkPortAvailability $sparkTSPort 2>/dev/null; then
			{ set +x; } 2>/dev/null
			registerNetworkPort spark_thriftserver $sparkTSPort 2>/dev/null
		else
			{ set +x; } 2>/dev/null
			logWarn "Spark-thriftServer port already has been taken by $(whoHasNetworkPort $sparkTSPort)"
		fi

		if [ "$isSparkTSPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] ; then
			changeWardenConfig "service.port" "service.port=$sparkTSPort" "thriftserver"
			sed -i "s/hive.server2.thrift.port.*/hive.server2.thrift.port=$sparkTSPort/" $SPARK_HOME/warden/warden.spark-thriftserver.conf
		fi

		if [ "$isSparkTSUIPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] ; then
			changeWardenConfig "service.ui.port" "service.ui.port=$sparkTSUIPort" "thriftserver"
		fi
	fi
}

function registerPortHistoryServer() {
	if [ -f $SPARK_HOME/warden/warden.spark-historyserver.conf ] ; then
		if checkNetworkPortAvailability $sparkHSUIPort 2>/dev/null; then
			{ set +x; } 2>/dev/null
			registerNetworkPort spark_historyServer $sparkHSUIPort 2>/dev/null
		else
			{ set +x; } 2>/dev/null
			logWarn "Spark-historyServer port already has been taken by $(whoHasNetworkPort $sparkHSUIPort)"
		fi

		if [ "$isSparkHSUIPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] || ! grep -q "spark.yarn.historyServer.address" "$SPARK_HOME/conf/spark-defaults.conf"; then
			changeSparkDefaults "spark.yarn.historyServer.address" "spark.yarn.historyServer.address $(hostname --fqdn):$sparkHSUIPort"
		fi

		if [ "$isSparkHSUIPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] || ! grep -q "spark.history.ui.port" "$SPARK_HOME/conf/spark-defaults.conf"; then
			changeSparkDefaults "spark.history.ui.port" "spark.history.ui.port $sparkHSUIPort"
		fi

		if [ "$isSparkHSUIPortDef" = true ] || [ "$IS_FIRST_RUN" = true ] ; then
			changeWardenConfig "service.ui.port" "service.ui.port=$sparkHSUIPort" "historyserver"
		fi
	fi
}

function registerServicePorts() {
	registerPortMaster
	registerPortThriftServer
	registerPortHistoryServer
}

#
# Add warden files
#

function copyWardenFile() {
	if [ -f $SPARK_HOME/warden/warden.spark-$1.conf ] ; then
		cp "${SPARK_HOME}/warden/warden.spark-${1}.conf" "${MAPR_CONF_DIR}/conf.d/" 2>/dev/null || :
	fi
}

function copyWardenConfFiles() {
	mkdir -p "$MAPR_HOME"/conf/conf.d
	copyWardenFile master
	copyWardenFile historyserver
	copyWardenFile thriftserver
}

function stopService() {
	if [ -e ${MAPR_CONF_DIR}/conf.d/warden.spark-${1}.conf ]; then
		logInfo "Stopping spark-$1..."
		${SPARK_HOME}/sbin/stop-${2}.sh
	fi
}

function stopServicesForRestartByWarden() {
	stopService master master
	stopService historyserver history-server
	stopService thriftserver thriftserver
}

#
# Parse options
#

USAGE="usage: $0 [-s|--secure || -u|--unsecure || -cs|--customSecure] [-R] [--EC] [-h|--help]]"

{ OPTS=`getopt -n "$0" -a -o suhR --long secure,unsecure,customSecure,help,EC,sparkHSUIPort:,sparkMasterPort:,sparkTSPort:,sparkMasterUIPort:,sparkTSUIPort: -- "$@"`; } 2>/dev/null

eval set -- "$OPTS"

while [ ${#} -gt 0 ] ; do
  case "$1" in
    --secure|-s)
      isSecure=1;
      shift 1;;
    --unsecure|-u)
      isSecure=0;
      shift 1;;
    --customSecure|-cs)
      if [ -f "$SPARK_HOME/etc/.not_configured_yet" ]; then
      	isSecure=1;
      else
      	isSecure=2;
      fi
      shift 1;;
     --R|-R)
      SPARK_IS_READY=true;
      shift;;
    --help|-h)
      echo "${USAGE}"
      exit $RETURN_SUCCESS
      ;;
    --EC|-EC)
      #ignoring
      shift;;
    --sparkHSUIPort)
      sparkHSUIPort=$2
      isSparkHSUIPortDef=true
      shift 2
      ;;
    --sparkMasterPort)
      sparkMasterPort=$2
      isSparkMasterPortDef=true
      shift 2
      ;;
    --sparkTSPort)
      sparkTSPort=$2
      isSparkTSPortDef=true
      shift 2
      ;;
    --sparkMasterUIPort)
      sparkMasterUIPort=$2
      isSparkMasterUIPortDef=true
      shift 2
      ;;
    --sparkTSUIPort)
      sparkTSUIPort=$2
      isSparkTSUIPortDef=true
      shift 2
      ;;
    --)
      shift; break;;
    *)
      # Invalid arguments passed
      break;;
  esac
done

configureOnHive
registerServicePorts
if [ ! "$isSecure" -eq 2 ] ; then
	configureSecurity
fi
change_permissions
copyWardenConfFiles
stopServicesForRestartByWarden

rm -f "$SPARK_HOME"/etc/.not_configured_yet

exit $RETURN_SUCCESS
