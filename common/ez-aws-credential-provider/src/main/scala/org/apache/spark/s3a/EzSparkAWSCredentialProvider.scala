package org.apache.spark.s3a

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import org.apache.commons.io.monitor.{FileAlterationListener, FileAlterationMonitor, FileAlterationObserver}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import java.io.File
import java.net.URI
import scala.io.Source._
import scala.util.Properties._

class EzSparkAWSCredentialProvider(uri: URI, conf: Configuration) extends AWSCredentialsProvider with Logging {

  private val DefaultAccessKeyLocation = "/etc/secrets/ezua/.auth_token"
  private val DefaultSecretKeyLocation = ""

  private var accessKey = ""
  private var secretKey = ""

  refresh()
  startKeyUpdateMonitor()

  override def getCredentials: AWSCredentials = {
    new BasicAWSCredentials(accessKey, secretKey)
  }

  override def refresh(): Unit = {
    accessKey = getUpdatedAccessKey()
    secretKey = getUpdatedSecretKey()
  }

  private def getUpdatedAccessKey(): String = {
    logInfo(s"Updating accessKey")
    val accessKeyLocation: String = envOrElse("AWS_ACCESS_KEY_LOCATION", DefaultAccessKeyLocation)
    getKeyFromLocation(accessKeyLocation)
  }

  private def getUpdatedSecretKey(): String = {
    logInfo(s"Updating secretKey")
    val secretKeyLocation: String = envOrElse("AWS_SECRET_KEY_LOCATION", DefaultSecretKeyLocation)
    try {
      getKeyFromLocation(secretKeyLocation)
    } catch {
      case _: AmazonClientException => "defaultSecretKeyValue"
    }
  }

  private def getKeyFromLocation(keyLocation: String): String = {
    if (keyLocation.isEmpty) {
      throw new AmazonClientException("Key path is not set")
    } else {
      logInfo(s"Reading key from location: $keyLocation")
      val keySource = fromFile(keyLocation)
      try keySource.mkString.trim finally keySource.close()
    }
  }

  private def startKeyUpdateMonitor(): Unit = {
    val accessKeyLocation = envOrElse("AWS_ACCESS_KEY_LOCATION", DefaultAccessKeyLocation)
    val accessKeyFile = new File(accessKeyLocation)

    val observer = new FileAlterationObserver(accessKeyFile.getParent)

    observer.addListener(new FileAlterationListener {
      override def onFileChange(file: File): Unit = {
        if (file.getName == accessKeyFile.getName) {
          logInfo(s"Access key file changed: ${file.getName}")
          refresh()
        }
      }
      override def onFileCreate(file: File): Unit = {
        if (file.getName == accessKeyFile.getName) {
          logInfo(s"Access key file created: ${file.getName}")
          refresh()
        }
      }
      override def onFileDelete(file: File): Unit = {}
      override def onDirectoryChange(directory: File): Unit = {}
      override def onDirectoryCreate(directory: File): Unit = {}
      override def onDirectoryDelete(directory: File): Unit = {}
      override def onStart(observer: FileAlterationObserver): Unit = {}
      override def onStop(observer: FileAlterationObserver): Unit = {}
    })

    val monitor = new FileAlterationMonitor(10000, observer)

    try {
      monitor.start()
    } catch {
      case e: Exception => logError("Error starting file monitor", e)
    }
  }
}
