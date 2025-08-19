%undefine __check_files

summary:     HPE DataFabric Ecosystem Pack: Apache Spark Connect Server
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-spark-connectserver
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   noarch
requires:    mapr-spark = __RELEASE_VERSION__
AutoReqProv: no

%description
Apache Spark Connect Server distribution included in HPE DataFabric Software Ecosystem Pack
Tag: __RELEASE_BRANCH__
Commit: __GIT_COMMIT__


%clean
echo "NOOP"


%files
__PREFIX__/roles


%post
echo "post-install called with argument \`$1'" >&2

SPARK_VERSION=__VERSION_3DIGIT__
MAPR_SPARK_HOME=$(dir -d -1 __PREFIX__/spark/spark-$SPARK_VERSION 2> /dev/null | tail -1)

cp $MAPR_SPARK_HOME/warden/warden.spark-connectserver.conf.template \
$MAPR_SPARK_HOME/warden/warden.spark-connectserver.conf

if [ "$1" = "1" ] ; then
  echo $SPARK_VERSION > __PREFIX__/spark/sparkversion
fi

%preun
SPARK_VERSION=__VERSION_3DIGIT__
MAPR_SPARK_HOME=$(dir -d -1 __PREFIX__/spark/spark-$SPARK_VERSION 2> /dev/null | tail -1)
echo "pre-uninstall called with argument \`$1'" >&2
echo "stopping any alive Spark-ConnectServer daemon on the node"
rm -f __PREFIX__/conf/conf.d/warden.spark-connectserver.conf
if [ $1 -eq 0 ]; then
	rm -f $MAPR_SPARK_HOME/warden/warden.spark-connectserver.conf
fi
if [ -f "$MAPR_SPARK_HOME/sbin/stop-connect-server.sh" ]; then
	$MAPR_SPARK_HOME/sbin/stop-connect-server.sh
fi

%posttrans
SPARK_VERSION=__VERSION_3DIGIT__
echo "post-transaction called with argument \`$1'" >&2
echo $SPARK_VERSION > __PREFIX__/spark/sparkversion
