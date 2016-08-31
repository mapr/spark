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

    set +x
    echo "$MAPR_SPARK_USER:$MAPR_SPARK_PASSWORD" | chpasswd
    set -x

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

         # FIXME: (Copy Metrics ticket)
         METRICS_TICKET_DIR=$(dirname "${MAPR_TICKETFILE_LOCATION}")
         METRICS_TICKET_FILE=$METRICS_TICKET_DIR/METRICS_TICKET
         cp $METRICS_TICKET_FILE $MAPR_HOME/conf/mapruserticket
    else
         MAPR_SECURE="-unsecure"
    fi

    local args="-no-autostart -on-prompt-cont y -v -f -nocerts"
    /opt/mapr/server/configure.sh $MAPR_SECURE $args -c -C $MAPR_CLDB_HOSTS -Z $MAPR_ZK_HOSTS -N $MAPR_CLUSTER -OT $MAPR_TSDB_HOSTS -ES $MAPR_ES_HOSTS

    # Configure collectd
    echo "Configuring collectd.."
    echo $MAPR_CLUSTER_ID >> $MAPR_HOME/conf/clusterid
    /opt/mapr/collectd/collectd-${collectd_version}/bin/configure.sh $MAPR_SECURE -nocerts -OT "$MAPR_TSDB_HOSTS"
}

function configureK8SProperties() {
    cat >> $SPARK_CONF_PATH <<EOM
spark.hadoop.yarn.resourcemanager.ha.custom-ha-enabled  false
spark.hadoop.yarn.resourcemanager.recovery.enabled      false
EOM

    if [ "$SECURE_CLUSTER" == "true" ]; then
        configureSecureK8SProperties
    fi

    # copy k8s metric properties
    sparkMetricsProperties=$SPARK_CONF_DIR/metrics.properties
    sparkMetricsPropertiesTemplate=${sparkMetricsProperties}.template
    if [ -f ${sparkMetricsPropertiesTemplate} ]; then
        cp ${sparkMetricsPropertiesTemplate} ${sparkMetricsProperties}
    fi

    # copy k8s submit properties to spark-defaults
    defaultK8SConfMount=/opt/spark/conf/spark.properties
    if [ -f ${defaultK8SConfMount} ]; then
        cat ${defaultK8SConfMount} >> ${SPARK_CONF_PATH}
    fi
}


function configureSecureK8SProperties() {
    cat >> $SPARK_CONF_PATH <<EOM
spark.ui.filters  org.apache.spark.ui.filters.MultiauthWebUiFilter
EOM
}

function startCollectdForMetrics() {

    if [ -v SPARK_EXECUTOR_ID ]; then
      echo "Skipping collectd for spark executor pod.."
    else
      echo "Starting CollectD for spark driver pod.."
      local collectd="/opt/mapr/collectd/collectd-${collectd_version}/etc/init.d/collectd"

      if [ ! -f $collectd ]; then
          echo "Could not find collectd start file at: ${collectd}"
      else
          ${collectd} start &
      fi
    fi
}

function configurePod() {
    validateUserCredentials
    createUser
    createUserGroups
    copySecurity
    configureSecurity
    startCollectdForMetrics
    configureK8SProperties
}
