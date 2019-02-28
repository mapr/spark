#!/usr/bin/env bash


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

function configurePod() {
    createUser
    createUserGroups
    copySecurity

    MAPR_USER=${MAPR_SPARK_USER}
    MAPR_GROUP=${MAPR_SPARK_GROUP}

    if [ "$SECURE_CLUSTER" == "true" ] ; then
        /opt/mapr/server/configure.sh -c -C $MAPR_CLDB_HOSTS -Z $MAPR_ZK_HOSTS -N $MAPR_CLUSTER -secure
        ${SPARK_HOME}/bin/configure.sh -c --secure
    else
        /opt/mapr/server/configure.sh -c -C $MAPR_CLDB_HOSTS -Z $MAPR_ZK_HOSTS -N $MAPR_CLUSTER
    fi

    # copy k8s submit properties to spark-defaults
    defaultK8SConfMount=/opt/spark/conf/spark.properties
    if [ -f ${defaultK8SConfMount} ]; then
        cat ${defaultK8SConfMount} >> $SPARK_CONF_PATH
    fi

    #Configure logger info level
    sed 's/rootCategory=WARN/rootCategory=INFO/' "$SPARK_CONF_DIR/log4j.properties"
}
