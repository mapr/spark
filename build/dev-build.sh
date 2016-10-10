#!/usr/bin/env bash
if [[ -z "$1" ]]; then
    ./build/mvn -Pyarn -Phadoop-provided -Pscala-2.10 -Phive -Phive-thriftserver -DskipTests clean install
else
    ./build/mvn -Pyarn -Phadoop-provided -Pscala-2.10 -Phive -Phive-thriftserver -DskipTests -pl :$1 clean install
    if [ $? -ne 0 ]; then exit 1; fi
    ./build/mvn -Pyarn -Phadoop-provided -Pscala-2.10 -Phive -Phive-thriftserver -DskipTests -pl :spark-assembly_2.10 clean package
fi

if [ $? -ne 0 ]; then exit 1; fi

scp assembly/target/scala-2.10/spark-assembly-1.6.1-hadoop2.7.0-mapr-1602.jar mapr@node1:/opt/mapr/spark/spark-1.6.1/lib
if [ $? -ne 0 ]; then exit 1; fi