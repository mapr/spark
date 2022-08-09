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

MAPR_CONF_DIR=${MAPR_CONF_DIR:-"$MAPR_HOME/conf"}
SPARK_VERSION=`cat $MAPR_HOME/spark/sparkversion`
LATEST_SPARK_TIMESTAMP=0

# Hive properties
HIVE_INSTALLED=false
if [ -f $MAPR_HOME/hive/hiveversion ]; then
	HIVE_INSTALLED=true
fi
if [ "$HIVE_INSTALLED" = true ]; then
	HIVE_VERSION=`cat $MAPR_HOME/hive/hiveversion`
	HIVE_HOME="$MAPR_HOME"/hive/hive-"$HIVE_VERSION"
fi

HBASE_INSTALLED=false
if [ -f $MAPR_HOME/hbase/hbaseversion ]; then
	HBASE_INSTALLED=true
fi
if [ "$HBASE_INSTALLED" = true ]; then
	HBASE_VERSION=`cat $MAPR_HOME/hbase/hbaseversion`
	HBASE_HOME="$MAPR_HOME"/hbase/hbase-"$HBASE_VERSION"
fi

SPARK_HOME="$MAPR_HOME"/spark/spark-"$SPARK_VERSION"
SPARK_CONF="$SPARK_HOME"/conf
SPARK_BIN="$SPARK_HOME"/bin
SPARK_LOGS="$SPARK_HOME"/logs
DAEMON_CONF=${MAPR_HOME}/conf/daemon.conf
DEFAULT_SSL_KEYSTORE="$MAPR_HOME"/conf/ssl_keystore

if [ -f /proc/sys/crypto/fips_enabled ]; then
  FIPS_ENABLED=$(cat /proc/sys/crypto/fips_enabled)
else
  FIPS_ENABLED="0"
fi

MAPR_USER=${MAPR_USER:-$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)}
MAPR_GROUP=${MAPR_GROUP:-$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)}

. ${MAPR_HOME}/server/common-ecosystem.sh 2> /dev/null # prevent verbose output, set by 'set -x'
if [ $? -ne 0 ]; then
  echo 'Error: Seems that MAPR_HOME is not correctly set or mapr-core is not installed.'
  exit 1
fi 2> /dev/null
{ set +x; } 2>/dev/null

initCfgEnv

CLUSTER_INFO=`cat $MAPR_HOME/conf/mapr-clusters.conf`

IS_FIRST_RUN=false
if [ -f $SPARK_HOME/etc/.not_configured_yet ] ; then
	IS_FIRST_RUN=true
fi
JUST_UPDATED=false
if [ -f $SPARK_HOME/etc/.just_updated ] ; then
	JUST_UPDATED=true
fi

declare -a SPARK_CONF_FILES=("$SPARK_CONF/spark-defaults.conf" "$SPARK_CONF/spark-env.sh" "$SPARK_CONF/hive-site.xml" "$SPARK_CONF/log4j2.properties")

# Spark ports
sparkHSUIPort=18080
isSparkHSUIPortDef=false
sparkTSPort=2304
isSparkTSPortDef=false
sparkTSUIPort=4040
isSparkTSUIPortDef=false
sparkMasterPort=7077
isSparkMasterPortDef=false
sparkMasterUIPort=8580
isSparkMasterUIPortDef=false

# secure ui ports
sparkMasterSecureUIPort=8980
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
# Create spark-defaults.conf from spark-defaults.conf.template
#
if [ -f $SPARK_HOME/conf/spark-defaults.conf.template ] && [ ! -f $SPARK_HOME/conf/spark-defaults.conf ] ; then
	cp "$SPARK_HOME/conf/spark-defaults.conf.template" "$SPARK_HOME/conf/spark-defaults.conf"
fi

#
# Create jetty.headers.xml from jetty.headers.xml.template
#
if [ -f $SPARK_HOME/conf/jetty.headers.xml.template ] && [ ! -f $SPARK_HOME/conf/jetty.headers.xml ] ; then
	cp "$SPARK_HOME/conf/jetty.headers.xml.template" "$SPARK_HOME/conf/jetty.headers.xml"
fi

#
# Create metrics.properties from metrics.properties.template
#
if [ -f $SPARK_HOME/conf/metrics.properties.template ] && [ ! -f $SPARK_HOME/conf/metrics.properties ] ; then
	cp "$SPARK_HOME/conf/metrics.properties.template" "$SPARK_HOME/conf/metrics.properties"
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
sed 's/rootCategory=INFO/rootCategory=WARN/' "$SPARK_HOME/conf/log4j2.properties.template" > "$SPARK_HOME/conf/log4j2.properties"

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
if grep -q "spark.ssl.standalone.port" "$SPARK_HOME/conf/spark-defaults.conf"; then
	sparkMasterSecureUIPort=$(sed -n -e '/^spark.ssl.standalone.port/p' $SPARK_HOME/conf/spark-defaults.conf | sed 's/.* //')
fi
if grep -q "spark.ssl.historyServer.port" "$SPARK_HOME/conf/spark-defaults.conf"; then
	sparkHSSecureUIPort=$(sed -n -e '/^spark.ssl.historyServer.port/p' $SPARK_HOME/conf/spark-defaults.conf | sed 's/.* //')
fi
if [ -f $SPARK_HOME/warden/warden.spark-historyserver.conf ] ; then
	changeSparkDefaults "spark.yarn.historyServer.address" "spark.yarn.historyServer.address $(hostname --fqdn):$sparkHSUIPort"
fi

sed -i '/# SECURITY BLOCK/,/# END OF THE SECURITY CONFIGURATION BLOCK/d' "$SPARK_HOME"/conf/spark-defaults.conf

if [ "$isSecure" == 1 ] ; then
	source $MAPR_HOME/conf/env.sh
    cat >> "$SPARK_HOME"/conf/spark-defaults.conf << EOF
# SECURITY BLOCK
# ALL SECURITY PROPERTIES MUST BE PLACED IN THIS BLOCK

# ssl
spark.ssl.enabled true
spark.ssl.fs.enabled true
spark.ssl.protocol TLSv1.2

# - PAM
spark.ui.filters  org.apache.spark.ui.filters.MultiauthWebUiFilter, org.apache.spark.ui.filters.CustomHeadersFilter

# - ACLS
spark.acls.enable       false
spark.admin.acls        mapr
spark.admin.acls.groups mapr
spark.ui.view.acls      mapruser1
spark.ui.headers $SPARK_CONF/jetty.headers.xml

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
EOF
	if [ -f $SPARK_HOME/warden/warden.spark-master.conf ] ; then
		changeWardenConfig "service.ui.port" "service.ui.port=$sparkMasterSecureUIPort" "master"
		sed -i "/\# ssl/a spark.ssl.standalone.port $sparkMasterSecureUIPort" $SPARK_HOME/conf/spark-defaults.conf
		sed -i "/\# ssl/a spark.ssl.standalone.keyStore $DEFAULT_SSL_KEYSTORE" $SPARK_HOME/conf/spark-defaults.conf
	fi
	if [ -f $SPARK_HOME/warden/warden.spark-historyserver.conf ] ; then
		changeWardenConfig "service.ui.port" "service.ui.port=$sparkHSSecureUIPort" "historyserver"
		sed -i "/\# ssl/a spark.ssl.historyServer.port $sparkHSSecureUIPort" $SPARK_HOME/conf/spark-defaults.conf
		sed -i "/\# ssl/a spark.ssl.historyServer.keyStore $DEFAULT_SSL_KEYSTORE" $SPARK_HOME/conf/spark-defaults.conf
		changeSparkDefaults "spark.yarn.historyServer.address" "spark.yarn.historyServer.address $(hostname --fqdn):$sparkHSSecureUIPort"
	fi
	if [ "$FIPS_ENABLED" = "1" ] ; then
	  sed -i "/\# ssl/a spark.ssl.keyStoreType bcfks" $SPARK_HOME/conf/spark-defaults.conf
	  sed -i "/\# ssl/a spark.ssl.trustStoreType bcfks" $SPARK_HOME/conf/spark-defaults.conf
	  sed -i 's/java.util=ALL-UNNAMED/java.util=ALL-UNNAMED -Djava.security.properties=\/opt\/mapr\/conf\/java.security.fips/g' ${SPARK_HOME}/conf/spark-defaults.conf
	fi
	if ! (echo "$CLUSTER_INFO" | grep -q "kerberosEnable=true") ; then
		if [ ! -f $SPARK_HOME/conf/hive-site.xml ] ; then
			cp $SPARK_HOME/conf/hive-site.xml.security.template $SPARK_HOME/conf/hive-site.xml
		else
			if ! grep -q \>hive.server2.thrift.sasl.qop\< "$SPARK_HOME/conf/hive-site.xml"; then
			  java -cp $SPARK_HOME'/jars/*' org.apache.spark.editor.HiveSiteEditor create hive.server2.thrift.sasl.qop=auth-conf
			fi
		fi

		if [ -f ${SPARK_HOME}/conf/spark-env.sh ] ; then
			sed -i 's/-Dhadoop.login=hybrid/-Dhadoop.login=maprsasl/g' ${SPARK_HOME}/conf/spark-env.sh
		fi
	fi
fi
}

function createAppsSparkFolder() {
  if [ -z "$MAPR_TICKETFILE_LOCATION" ]; then
    isSecured="false"
    if [ -e "${MAPR_HOME}/conf/mapr-clusters.conf" ]; then
      isSecured=$(head -n1 "${MAPR_HOME}/conf/mapr-clusters.conf" | grep -o 'secure=\w*' | cut -d '=' -f 2)
    fi
    if [ "$isSecured" = "true" ] && [ -e "${MAPR_HOME}/conf/mapruserticket" ]; then
      export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
    fi
  fi
	hadoop fs -mkdir -p /apps/spark > /dev/null 2>&1
	hadoop fs -chmod 777 /apps/spark > /dev/null 2>&1
}

#
# Configure on Hive
#

function configureOnHive() {
	if [ -f $HIVE_HOME/conf/hive-site.xml ]; then
		if [ ! "$isSecure" -eq 2 ] || [ "$IS_FIRST_RUN" = true ]; then
			cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml
		fi
	fi
	if [ -f $SPARK_HOME/conf/hive-site.xml ] ; then
		java -cp $SPARK_HOME'/jars/*' org.apache.spark.editor.HiveSiteEditor replace hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory hive.execution.engine=mr
	fi
}

#
# Configure on Hbase
#

function configureOnHbase() {
    if [ -f $HBASE_HOME/conf/hbase-site.xml ]; then
        cp $HBASE_HOME/conf/hbase-site.xml $SPARK_HOME/conf/hbase-site.xml
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

		if [ "$isSparkTSPortDef" = true ] ; then
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
is_warden_file_already_copied() {
  local role=$1
  [ -f "$MAPR_CONF_DIR/conf.d/warden.spark-${role}.conf" ]
}

function copyWardenFile() {
        if [ -f $SPARK_HOME/warden/warden.spark-$1.conf ] && ! is_warden_file_already_copied "$1" ; then
                logInfo "Copying warden.spark-${1}.conf file"
                cp "${SPARK_HOME}/warden/warden.spark-${1}.conf" "${MAPR_CONF_DIR}/conf.d/" 2>/dev/null || :
        fi
}

function copyWardenConfFiles() {
	mkdir -p "$MAPR_HOME"/conf/conf.d
	copyWardenFile master
	copyWardenFile historyserver
	copyWardenFile thriftserver
}

function mkBackupForOldConfigs() {
	for i in "${SPARK_CONF_FILES[@]}"
	do
		if [ -f ${i} ]  ; then
			cp "$i" "$i.old"
		fi
	done
}

function stopService() {
	if [ -e ${MAPR_CONF_DIR}/conf.d/warden.spark-${1}.conf ]; then
		logInfo "Stopping spark-$1..."
		${SPARK_HOME}/sbin/stop-${2}.sh
	fi
}

function stopServicesForRestartByWarden() {
	if [ -f $SPARK_HOME/conf/spark-defaults.conf ] && [ -f $SPARK_HOME/conf/spark-defaults.conf.old ] ; then
		spark_defaults_diff=`diff ${SPARK_HOME}/conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf.old; echo $?`
	fi
	if [ -f $SPARK_HOME/conf/spark-env.sh ] && [ -f $SPARK_HOME/conf/spark-env.sh.old ] ; then
		spark_env_sh_diff=`diff ${SPARK_HOME}/conf/spark-env.sh ${SPARK_HOME}/conf/spark-env.sh.old; echo $?`
	fi
	if [ -f $SPARK_HOME/conf/hive-site.xml ] && [ -f $SPARK_HOME/conf/hive-site.xml.old ] ; then
		spark_hive_site_diff=`diff ${SPARK_HOME}/conf/hive-site.xml ${SPARK_HOME}/conf/hive-site.xml.old; echo $?`
	fi
	if [ -f $SPARK_HOME/conf/log4j2.properties ] && [ -f $SPARK_HOME/conf/log4j2.properties.old ] ; then
		spark_log4j_diff=`diff ${SPARK_HOME}/conf/log4j2.properties ${SPARK_HOME}/conf/log4j2.properties.old; echo $?`
	fi
	if [ ! "$spark_defaults_diff" = "0" ] || [ ! "$spark_env_sh_diff" = "0" ] || [ ! "$spark_hive_site_diff" = "0" ] || [ ! "$spark_log4j_diff" = "0" ]; then
		stopService master master
		stopService historyserver history-server
		stopService thriftserver thriftserver
	fi
}

function findLatestTimestamp() {
	if [ $(find "$MAPR_HOME"/spark/ -type d -name "spark-$SPARK_VERSION.*" | wc -l ) != "0" ] ; then
		for file in /opt/mapr/spark/spark-${SPARK_VERSION}.*; do
        	if [ ${file##*.} -gt ${LATEST_SPARK_TIMESTAMP} ] ; then
        		LATEST_SPARK_TIMESTAMP=${file##*.}
       		fi
		done
	fi
}

function replaceConfigFromPreviousVersion() {
	findLatestTimestamp
	if [ ${LATEST_SPARK_TIMESTAMP} -ne 0 ] ; then
		cp ${SPARK_HOME}.*${LATEST_SPARK_TIMESTAMP}/conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
		cp ${SPARK_HOME}.*${LATEST_SPARK_TIMESTAMP}/conf/spark-env.sh ${SPARK_HOME}/conf/spark-env.sh
	fi
}

#
# Parse options
#

USAGE="usage: $0 [-s|--secure || -u|--unsecure || -cs|--customSecure] [-R] [--EC <common args>] [-h|--help]]"

{ OPTS=`getopt -n "$0" -a -o suhR --long secure,unsecure,customSecure,help,EC:,sparkHSUIPort:,sparkMasterPort:,sparkTSPort:,sparkMasterUIPort:,sparkTSUIPort: -- "$@"`; } 2>/dev/null

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
      shift 2;;
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

registerServicePorts
if [ "$HIVE_INSTALLED" = true ]; then
	configureOnHive
fi
if [ "$HBASE_INSTALLED" = true ]; then
	configureOnHbase
fi
if [ ! "$isSecure" -eq 2 ] ; then
	configureSecurity
fi
createAppsSparkFolder
change_permissions
mkBackupForOldConfigs

copyWardenConfFiles
stopServicesForRestartByWarden

if [ "$JUST_UPDATED" = true ] ; then
	replaceConfigFromPreviousVersion
	rm -f "$SPARK_HOME"/etc/.just_updated
fi

rm -f "$SPARK_HOME"/etc/.not_configured_yet

exit $RETURN_SUCCESS
