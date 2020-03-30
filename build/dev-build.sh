#!/usr/bin/env bash

PROFILES="-Pyarn -Phadoop-provided -Pscala-2.11 -Phive -Phive-thriftserver -DskipTests"

if [[ -z "$1" ]]; then
    ./build/mvn ${PROFILES} clean install
else
    ./build/mvn ./build/mvn ${PROFILES} -pl :$1 clean install
    if [ $? -ne 0 ]; then exit 1; fi
    ./build/mvn ${PROFILES} -pl :spark-assembly_2.10 clean package
fi

if [ $? -ne 0 ]; then exit 1; fi

scp -r assembly/target/scala-2.11/jars mapr@node1:/opt/mapr/spark/spark-2.4.5/jars
if [ $? -ne 0 ]; then exit 1; fi