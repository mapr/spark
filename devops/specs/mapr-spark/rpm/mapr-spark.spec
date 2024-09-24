%undefine __check_files
%define _binaries_in_noarch_packages_terminate_build 0


summary:     MapR
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise, <ezmeral_software_support@hpe.com>
name:        mapr-spark
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       MapR
buildarch:   noarch
Requires:    mapr-client, mapr-hadoop-client
AutoReqProv: no


%description
Ezmeral Ecosystem Pack: Spark package
Tag: __RELEASE_BRANCH__
Commit: __GIT_COMMIT__


%clean
echo "NOOP"


%files
__PREFIX__/roles
__PREFIX__/spark



%pre
OLD_TIMESTAMP=$(rpm -qi mapr-spark | awk -F': ' '/Version/ {print $2}')
OLD_VERSION="$( echo $OLD_TIMESTAMP| cut -d'.' -f1-3 )"
OLD_CONF_DIR=__PREFIX__/spark/spark-"$OLD_TIMESTAMP"
if [ "$1" = "2" ] ; then
  rm -f __PREFIX__/spark/sparkversion
  mkdir -p "$OLD_CONF_DIR"
  cp -r __PREFIX__/spark/spark-"$OLD_VERSION"/conf "$OLD_CONF_DIR"/
fi

%post
SPARK_VERSION=__VERSION_3DIGIT__
SPARK_HOME=__PREFIX__/spark/spark-${SPARK_VERSION}

echo "post-install called with argument \`$1'" >&2

ln -sfn $SPARK_HOME /usr/local/spark

case "$1" in
  1)
    touch "${SPARK_HOME}/conf/.not_configured_yet"
  ;;
  2)
     touch "${SPARK_HOME}/conf/.just_updated"
  ;;
esac


%preun
SPARK_VERSION=__VERSION_3DIGIT__
SPARK_HOME=__PREFIX__/spark/spark-${SPARK_VERSION}
echo "post-install called with argument \`$1'" >&2

# Uninstall should only stop slave on localhost and not honor conf/slaves file
export HOSTLIST=localhost
$SPARK_HOME/sbin/stop-slaves.sh
rm -rf __PREFIX__/shark/spark-$SPARK_VERSION/conf
rm -rf __PREFIX__/spark/shark-$SPARK_VERSION/conf

%postun

if [ $1 = 0 ]; then
    rm -rf __PREFIX__/spark
    rm -rf __PREFIX__/shark
    rm -rf /usr/local/spark
fi
if [ $1 -eq 1 ]; then
	SPARK_VERSION="__VERSION_3DIGIT__"
    NEW_TIMESTAMP=$(rpm -qi mapr-spark | awk -F': ' '/Version/ {print $2}')
    NEW_VERSION_WITH_TIMESTAMP="$( echo $NEW_TIMESTAMP| cut -d' ' -f2 )"
    NEW_VERSION="$( echo $NEW_VERSION_WITH_TIMESTAMP| cut -d'.' -f1-3 )"
    REMOVE_VERSION="$( echo $SPARK_VERSION| cut -d'.' -f1-3 )"
    if [ "$NEW_VERSION" != "$REMOVE_VERSION" ]; then
        rm -rf __INSTALL_3DIGIT__
    fi
fi

%posttrans
SPARK_VERSION=__VERSION_3DIGIT__
SPARK_HOME=__PREFIX__/spark/spark-${SPARK_VERSION}
echo "post-transaction called with argument \`$1'" >&2
echo $SPARK_VERSION > __PREFIX__/spark/sparkversion
