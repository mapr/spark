#!/bin/bash
# Source this script to determine the version of hadoop to use
# and attributes specific to that version.

# Sets attributes for a given hadoop mode
function set_hadoop_attr()
{
case "$1" in
	classic)
    hadoop_int_mode=1
    hadoop_version="${classic_version}"
    hadoop_home_dir=${INSTALL_DIR}/hadoop/hadoop-${hadoop_version}
    hadoop_conf_dir=${hadoop_home_dir}/conf
    ;;

	yarn)
    hadoop_int_mode=2
    hadoop_version="${yarn_version}"
    hadoop_home_dir=${INSTALL_DIR}/hadoop/hadoop-${hadoop_version}
    hadoop_conf_dir=${hadoop_home_dir}/etc/hadoop
    ;;

  *)
    echo "Invalid hadoop mode: $1"
    exit 1
    ;;
esac
}

# Main
INSTALL_DIR=/opt/mapr
HADOOP_VERSION_FILE=${INSTALL_DIR}/conf/hadoop_version
hadoop_mode=

#################################################
# The following attributes are set by this script
hadoop_int_mode=
hadoop_version=
hadoop_home_dir=
hadoop_conf_dir=
#################################################

# Source the version file. This file must exist on all installations.
. $HADOOP_VERSION_FILE

# Use the mode from version file
if [ ! -z "$default_mode" ]; then
 hadoop_mode=$default_mode
fi

# Check if we need to override using environment variable
if [ ! -z "$MAPR_MAPREDUCE_MODE" ]; then
  hadoop_mode=$MAPR_MAPREDUCE_MODE
fi

# Default to yarn mode if still nothing is set.
if [ -z "$hadoop_mode" ]; then
  hadoop_mode="yarn"
fi

set_hadoop_attr $hadoop_mode
