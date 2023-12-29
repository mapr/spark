package org.apache.spark.s3a

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import java.net.URI
import scala.io.Source._
import scala.util.Properties._

class EzSparkAWSCredentialProvider(uri: URI, conf: Configuration) extends AWSCredentialsProvider with Logging {

  private val DefaultAccessKeyLocation = "/etc/secrets/ezua/.auth_token"
  private val DefaultSecretKeyLocation = ""

  private var accessKey = ""
  private var secretKey = ""

  refresh()

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
}
