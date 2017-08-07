# Apache MapR Spark

This is the simple instruction on how to build the mapr released Spark code base for developer to create a tgz package.
This will also include the simple cherry pick example on how to pull commit from Apache Spark Github.com repo to your local repo (based on mapr released tagged tree)


## Building MapR edition of Spark

## Prerequisite
##   1) Java 7 or 8 depending on the version of Spark to be built
##   2) Maven 

################################################################################

## Clone/update the source tree
git clone git@github.com:mapr/spark
cd spark

## For the specific version of Spark, the following command will list all the released tags from MapR
git tag --list | grep mapr

## As an example for checking out 2.1.0-mapr-1707 release
git checkout 2.1.0-mapr-1707

## Build step
export MVN_PROFILE_ARG=-Pyarn,mesos,hadoop-provided,hive,hive-thriftserver,sparkr,include-kafka-09
./dev/make-distribution.sh --tgz

The tar archive will be generated in the same directory

Exmaple output tgz: spark-2.1.0-mapr-1707-bin-2.7.0-mapr-1607.tgz

################################################################################

## Cherry pick commit from upstream (Apache Spark):

git remote add spark git@github.com:apache/spark.git
git fetch spark

## Example of cherry pick one spark 2.1.0 commit
git log spark/branch-2.1
git cherry-pick d995dac1cdeec940364453675f59ce5cf2b53684
git push

./dev/make-distribution.sh --tgz

