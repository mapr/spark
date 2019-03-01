#!/usr/bin/env bash

function validateUserCredentials() {
    set +x
    MAPR_SPARK_USER=${MAPR_SPARK_USER:-mapr}
    MAPR_SPARK_GROUP=${MAPR_SPARK_GROUP:-mapr}
    MAPR_SPARK_PASSWORD=${MAPR_SPARK_PASSWORD:-mapr}
    MAPR_SPARK_UID=${MAPR_SPARK_UID:-5000}
    MAPR_SPARK_GID=${MAPR_SPARK_GID:-5000}
    set -x
}

function createUserGroups() {
  groups=($MAPR_SPARK_GROUP)
  groupIds=($MAPR_SPARK_GID)

  for i in "${!groups[@]}"
  do
    groupadd -f -g ${groupIds[i]} ${groups[i]}
    usermod -a -G  ${groups[i]} ${MAPR_SPARK_USER}
  done
}

function createUser() {
  if ! id ${MAPR_SPARK_USER} >/dev/null 2>&1; then
    adduser -u $MAPR_SPARK_UID ${MAPR_SPARK_USER} -m -d /home/${MAPR_SPARK_USER}
    echo "$MAPR_SPARK_USER:$MAPR_SPARK_PASSWORD" | chpasswd

    if [ -d /home/${MAPR_SPARK_USER} ]; then
      cd /home/${MAPR_SPARK_USER}
    fi
  fi
  chown ${MAPR_SPARK_USER} ./
}

function copySecurity() {
  if [ ! -z "$MAPR_SSL_LOCATION" ] ; then
    if [ -z "$MAPR_HOME" ] ; then
      MAPR_HOME=/home/mapr
    fi
    cp "$MAPR_SSL_LOCATION"/* $MAPR_HOME/conf/
  fi
}

function configureSecurity() {
    MAPR_USER=${MAPR_SPARK_USER}
    MAPR_GROUP=${MAPR_SPARK_GROUP}

    if [ "$SECURE_CLUSTER" == "true" ]; then
         MAPR_SECURE="-secure"
    else
         MAPR_SECURE="-unsecure"
    fi

    /opt/mapr/server/configure.sh -c -C $MAPR_CLDB_HOSTS -Z $MAPR_ZK_HOSTS -N $MAPR_CLUSTER -genkeys "${MAPR_SECURE}"
    ${SPARK_HOME}/bin/configure.sh -c "-${MAPR_SECURE}"
}

function configureK8SProperties() {
    cat >> $SPARK_CONF_PATH <<EOM
spark.authenticate false
spark.authenticate.enableSaslEncryption false
spark.io.encryption.enabled     false
spark.hadoop.yarn.resourcemanager.ha.custom-ha-enabled  false
spark.hadoop.yarn.resourcemanager.recovery.enabled      false
EOM

    # copy k8s submit properties to spark-defaults
    defaultK8SConfMount=/opt/spark/conf/spark.properties
    if [ -f ${defaultK8SConfMount} ]; then
        cat ${defaultK8SConfMount} >> $SPARK_CONF_PATH
    fi
}

function configureLogs() {
    log4jConf=$SPARK_CONF_DIR/log4j.properties
    if [ -f ${log4jConf} ]; then
        sed 's/rootCategory=WARN/rootCategory=INFO/' ${log4jConf}
    fi
}

function configurePod() {
    validateUserCredentials
    createUser
    createUserGroups
    copySecurity
    configureSecurity
    configureK8SProperties
    configureLogs
}
