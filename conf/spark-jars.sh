#!/bin/bash

SPARK_INSTALLATION_DIRECTORY="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
SPARK_JARS_CLASSPATH=$(find $SPARK_INSTALLATION_DIRECTORY/jars -name '*.jar' -printf '%p:' | sed 's/:$//')
