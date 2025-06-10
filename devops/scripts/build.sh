#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"
. "${SCRIPT_DIR}/_utils.sh"

build_spark() {
  export MVN_BUILD_COMMAND_ARGS

  export MVN_PROFILE_ARG
  ./dev/make-distribution.sh

  mkdir -p "${BUILD_ROOT}/build"
  mv -T "dist" "${BUILD_ROOT}/build"

  mkdir -p "${BUILD_ROOT}/build/mapr-util"
  cp ext-utils/* ext-conf/compatibility.version "${BUILD_ROOT}/build/mapr-util"

  mkdir -p "${BUILD_ROOT}/build/warden"
  cp ext-conf/warden.spark-historyserver.conf "${BUILD_ROOT}/build/warden/warden.spark-historyserver.conf.template"
  cp ext-conf/warden.spark-master.conf        "${BUILD_ROOT}/build/warden/warden.spark-master.conf.template"
  cp ext-conf/warden.spark-thriftserver.conf  "${BUILD_ROOT}/build/warden/warden.spark-thriftserver.conf.template"
  cp ext-conf/warden.spark-connectserver.conf  "${BUILD_ROOT}/build/warden/warden.spark-connectserver.conf.template"

  cp -rp ext-lib/scala "${BUILD_ROOT}/build"
}

main() {
  echo "Cleaning '${BUILD_ROOT}' dir..."
  rm -rf "$BUILD_ROOT"

  if [ "$DO_DEPLOY" = "true" ] && [ "$OS" = "redhat" ]; then
    echo "Deploy is enabled"
    export MVN_BUILD_COMMAND_ARGS="deploy $DEPLOY_ARGS"
  fi

  echo "Building project..."
  build_spark

  echo "Preparing directory structure..."
  setup_role "mapr-spark"
  setup_role "mapr-spark-historyserver"
  setup_role "mapr-spark-master"
  setup_role "mapr-spark-thriftserver"
  setup_role "mapr-spark-connectserver"

  setup_package "mapr-spark"

  echo "Building packages..."
  build_package "mapr-spark"
  build_package "mapr-spark-historyserver"
  build_package "mapr-spark-master"
  build_package "mapr-spark-thriftserver"
  build_package "mapr-spark-connectserver"

  echo "Resulting packages:"
  find "$DIST_DIR" -exec readlink -f {} \;
}

main
