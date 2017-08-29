#!/usr/bin/env bash

SPARK_HOME=$(readlink -f "/usr/local/spark")
java -cp $SPARK_HOME'/jars/*' org.apache.spark.classpath.ClasspathFilter $(mapr classpath) $SPARK_HOME'/conf/dep-blacklist.txt'
