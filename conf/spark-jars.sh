#!/bin/bash

SPARK_INSTALLATION_DIRECTORY="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SPARK_JARS_CLASSPATH="$SPARK_INSTALLATION_DIRECTORY/lib/spark-assembly-*.jar"
