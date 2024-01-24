%undefine __check_files

summary:     MapR
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise, <ezmeral_software_support@hpe.com>
name:        mapr-spark-historyserver
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       MapR
buildarch:   noarch
requires:    mapr-spark = __RELEASE_VERSION__
AutoReqProv: no

%description
Ezmeral Ecosystem Pack: Spark History Server package
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

cp $MAPR_SPARK_HOME/warden/warden.spark-historyserver.conf.template \
$MAPR_SPARK_HOME/warden/warden.spark-historyserver.conf

if [ "$1" = "1" ] ; then
  echo $SPARK_VERSION > __PREFIX__/spark/sparkversion
fi

%preun
SPARK_VERSION=__VERSION_3DIGIT__
MAPR_SPARK_HOME=$(dir -d -1 __PREFIX__/spark/spark-$SPARK_VERSION 2> /dev/null | tail -1)
echo "pre-uninstall called with argument \`$1'" >&2
echo "stopping any alive Spark-HistoryServer daemon on the node"
rm -f __PREFIX__/conf/conf.d/warden.spark-historyserver.conf
if [ $1 -eq 0 ]; then
	rm -f $MAPR_SPARK_HOME/warden/warden.spark-historyserver.conf
fi
if [ -f "$MAPR_SPARK_HOME/sbin/stop-history-server.sh" ]; then
	$MAPR_SPARK_HOME/sbin/stop-history-server.sh
fi
sed -i '/^spark.yarn.historyServer.address/ d' $MAPR_SPARK_HOME/conf/spark-defaults.conf 2> /dev/null

%posttrans
SPARK_VERSION=__VERSION_3DIGIT__
echo "post-transaction called with argument \`$1'" >&2
echo $SPARK_VERSION > __PREFIX__/spark/sparkversion
