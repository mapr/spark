#!/bin/bash

GIT_COMMIT=$(git log -1 --pretty=format:"%H")
INSTALLATION_PREFIX=${INSTALLATION_PREFIX:-"/opt/mapr"}
PKG_NAME=${PKG_NAME:-"spark"}
PKG_VERSION=${PKG_VERSION:-"3.5.1.0"}
PKG_3DIGIT_VERSION=$(echo "$PKG_VERSION" | cut -d '.' -f 1-3)
TIMESTAMP=${TIMESTAMP:-$(sh -c 'date "+%Y%m%d%H%M"')}
PKG_INSTALL_ROOT=${PKG_INSTALL_ROOT:-"${INSTALLATION_PREFIX}/${PKG_NAME}/${PKG_NAME}-${PKG_3DIGIT_VERSION}"}

DIST_DIR=${DIST_DIR:-"devops/dist"}
# rpmbuild does not work properly when relatve path specified here
BUILD_ROOT=${BUILD_ROOT:-"$(pwd)/devops/buildroot"}

MVN_PROFILE_ARG=${MVN_PROFILE_ARG:-"-Pyarn,hadoop-provided,hadoop-3.3,scala-2.12,hive,hive-thriftserver,sparkr,kubernetes,include-kafka,include-kafka-sql,include-maprdb"}
DEPLOY_ARGS=${DEPLOY_ARGS:-"
    -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dscalastyle.failOnViolation=false -Dscalatest.testFailureIgnore=true
    -DaltDeploymentRepository=mapr-snapshots::default::${MAPR_MAVEN_REPO}
"}
