#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"
. "${SCRIPT_DIR}/_utils.sh"

build_spark() {
  export MVN_BUILD_COMMAND_ARGS

  export MVN_PROFILE_ARG
  ./dev/make-distribution.sh

  mkdir -p "${BUILD_DIR}/build"
  mv -T "dist" "${BUILD_DIR}/build"

  # NOTE:
  # Be careful with the "dist" directory as it's used by both "make-distribution.sh" and our Jenkins,
  # which is expected to include only build artifacts (RPM/DEB packages).

  mkdir -p "${BUILD_DIR}/build/mapr-util"
  cp ext-utils/* ext-conf/compatibility.version "${BUILD_DIR}/build/mapr-util"

  mkdir -p "${BUILD_DIR}/build/warden"
  cp ext-conf/warden.spark-{historyserver,master,thriftserver}.conf "${BUILD_DIR}/build/warden"

  cp -rp ext-lib/scala "${BUILD_DIR}/build"
}

main() {
  echo "Cleaning '${BUILD_DIR}' and '${DIST_DIR}' dir..."
  rm -rf "$BUILD_DIR" "$DIST_DIR"

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

  setup_package "mapr-spark"

  echo "Building packages..."
  build_package "mapr-spark"
  build_package "mapr-spark-historyserver"
  build_package "mapr-spark-master"
  build_package "mapr-spark-thriftserver"

  echo "Resulting packages:"
  find "$DIST_DIR" -exec readlink -f {} \;
}

main
