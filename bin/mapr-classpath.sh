#!/bin/sh

if [ ! "$(command -v java)" ]; then
  echo "JAVA_HOME is not set" >&2
  exit 1
fi

SPARK_HOME=${SPARK_HOME:-"$(readlink -f /usr/local/spark)"}
mapr_classpath="$(mapr classpath)"
exec "${SPARK_HOME}/bin/classpathfilter" -b "${SPARK_HOME}/conf/dep-blacklist.txt" "$mapr_classpath"
