package org.apache.spark.ui

import java.net.InetAddress

import com.mapr.fs.MapRFileSystem
import org.apache.curator.framework.CuratorFramework
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging

private[spark] class SparkMetricsUtils

private[spark] object SparkMetricsUtils extends SparkMetricsUtils with Logging {

  private val sparkMetricsZkRoot = "/spark-metrics"
  private val metricsZkConf = "spark.metrics.zookeeper.url"

  def dumpMetricsURLToZookeeper(appId : String, url : String, port: Int, sparkConf: SparkConf): Option[CuratorFramework] = {
    if(port == -1) {
      logWarning(s"Cannot create metrics znode for unbound app: $appId")
      // No need to create znode for unbounded application
      return None
    }

    val fqdn = InetAddress.getLocalHost.getCanonicalHostName
    val data = s"$port,$url"
    val subFolder = s"$sparkMetricsZkRoot/$fqdn"
    val node = s"$subFolder/$port"

    val zkURLs = getZookeeperURLs
    sparkConf.set(metricsZkConf, zkURLs)
    val zk: CuratorFramework = SparkCuratorUtil.newClient(sparkConf, metricsZkConf)
    sparkConf.remove(metricsZkConf)

    mkdir(zk, sparkMetricsZkRoot)
    mkdir(zk, subFolder)
    zk.create.withProtectedEphemeralSequential.forPath(node, data.getBytes)

    Some(zk)
  }

  private def getZookeeperURLs(): String = {
    val mfs = FileSystem.get(new Configuration()).asInstanceOf[MapRFileSystem]
    mfs.getZkConnectString
  }

  private def mkdir(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) == null) {
      zk.create().creatingParentsIfNeeded().forPath(path)
    }
  }
}
