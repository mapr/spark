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
  private val sslTrustoreLocation = System.getProperty("user.home") + "/spark/security_keys/ssl_truststore"

  def dumpMetricsURLToZookeeper(appId : String,
                                url : String,
                                boundPort: Int,
                                securePort: Option[Int],
                                sparkConf: SparkConf): Option[CuratorFramework] = {
    if(boundPort == -1) {
      logWarning(s"Cannot create metrics znode for unbound app: $appId")
      // No need to create znode for unbounded application
      return None
    }

    val sslUiEnabled = !securePort.isEmpty

    val fqdn = InetAddress.getLocalHost.getCanonicalHostName
    val data = constractZNodeData(sslUiEnabled,
                                  boundPort,
                                  securePort,
                                  url,
                                  appId) match {
      case Some(data) => data
      case _ => return None
    }
    val subFolder = s"$sparkMetricsZkRoot/$fqdn"
    val node = s"$subFolder/$boundPort"

    val zkURLs = getZookeeperURLs
    sparkConf.set(metricsZkConf, zkURLs)
    val zk: CuratorFramework = SparkCuratorUtil.newClient(sparkConf, metricsZkConf)
    sparkConf.remove(metricsZkConf)

    mkdir(zk, sparkMetricsZkRoot)
    mkdir(zk, subFolder)
    zk.create.withProtectedEphemeralSequential.forPath(node, data.getBytes)

    Some(zk)
  }

  private def constractZNodeData(sslUiEnabled: Boolean,
                                 boundPort: Int,
                                 securePort: Option[Int],
                                 url: String,
                                 appId : String): Option[String] = {
    if (sslUiEnabled) {
      val pattern = "(http://)(.*):(\\d+)".r
      val secureUrl = url match {
        case pattern(_, fqdn, _) => s"https://$fqdn:${securePort.get}"
        case _ => {
          logWarning(s"Base url does not match the pattern: url=$url, pattern=$pattern . " +
          s"Cannot create metrics znode for app: $appId")
          return None
        }
      }
      Some(s"${securePort.get},$secureUrl,$sslTrustoreLocation")
    }
    else {
      Some(s"$boundPort,$url")
    }
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