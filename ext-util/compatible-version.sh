#!/bin/bash

# Output: A single version or comma separated list of versions
compatible_versions=

INSTALL_DIR="/opt/mapr"

# Loads the compatibility file for component 1 and sets the versions of
# component 2 that are compatible with it.

# If no entry exists for component 2, then a default value is set to indicate
# that any available version will work. This is needed to avoid having to add
# an entry for each component. The default is a special keyword "ANY".
function set_compatible_versions()
{
  # Inputs
  component_name_1=$1
  component_version_1=$2
  component_name_2=$3

  component_dir_1="$INSTALL_DIR/$component_name_1/$component_name_1-$component_version_1"
  compatible_versions=`cat "$component_dir_1/mapr-util/compatibility.version" | grep ${component_name_2} | awk -F= '{print $2}'`

  if [ -z $compatible_versions ]; then
    compatible_versions="ANY"
  fi
}
