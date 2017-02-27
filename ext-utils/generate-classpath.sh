#!/bin/bash

# Output: List of jars that need to be included to enable the given component.
# The jar paths are delimited by a colon. 
generated_classpath=

INSTALL_DIR="/opt/mapr"

# Iterates over versions of component 2 that are compatible with component 1
# version and constructs the classpath based on which version is available on
# the node.
function generate_compatible_classpath()
{
  # Input
  component_name_1=$1
  component_version_1=$2
  component_name_2=$3

  source $INSTALL_DIR/$component_name_1/$component_name_1-$component_version_1/mapr-util/compatible-version.sh
  set_compatible_versions $component_name_1 $component_version_1 $component_name_2

  for component_version_2 in $(echo $compatible_versions | tr "," "\n")
  do 
    generate_classpath $component_name_2 $component_version_2

    # Break out of loop if the version was available
    if [ ! -z "$generated_classpath" ]; then
      break
    fi
  done
}

# Generates the classpath for given component.
function generate_classpath()
{
  # Inputs
  component_name=$1
  component_version=$2

  case "$component_name" in
    hadoop)
      generate_hadoop_classpath
      ;;

    hive)
      generate_hive_classpath $component_version
      ;;

    hbase)
      generate_hbase_classpath $component_version
      ;;

    *)
      echo "ERROR: Classpath generation unsupported for $component_name"
      ;;
  esac
}

function generate_hadoop_classpath()
{
  generated_classpath=`hadoop classpath`
}

function generate_hive_classpath()
{
  component_name="hive"
  component_version="$1"
  component_lib_dir="$INSTALL_DIR/$component_name/$component_name-$component_version/lib"

  if [ -d $component_lib_dir ]; then
    # Adding all jars under hive lib dir since they are more than a handful
    generated_classpath="$component_lib_dir/*"
  fi
}

function generate_hbase_classpath()
{
  component_name="hbase"
  component_version="$1"
  component_lib_dir="$INSTALL_DIR/$component_name/$component_name-$component_version/lib"

  if [ -d $component_lib_dir ]; then
    add_glob_jar "$component_lib_dir/hbase-*.jar"
    add_glob_jar "$component_lib_dir/htrace-core*.jar"
    add_glob_jar "$component_lib_dir/metrics-core*.jar"
  fi
}

# Expands the given glob pattern and adds all the jars to classpath.
# Useful when we want to add jars with a certain prefix.
function add_glob_jar()
{
  jars=`ls $1 | tr "\n" ":"`
  generated_classpath=$generated_classpath:"$jars"
}
