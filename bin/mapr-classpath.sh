#!/usr/bin/env bash

if [ ! "$(command -v java)" ]; then
  echo "JAVA_HOME is not set" >&2
  exit 1
fi

SPARK_HOME=$(readlink -f "/usr/local/spark")
java -cp $SPARK_HOME'/jars/*' org.apache.spark.classpath.ClasspathFilter $(mapr classpath) $SPARK_HOME'/conf/dep-blacklist.txt'
