/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.Base64

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.sasl.SecretKeyHolder
import org.apache.spark.util.Utils

import scala.sys.process._
import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, Paths}

import com.mapr.fs.MapRFileSystem
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._
import org.apache.zookeeper.WatchedEvent

/**
 * Spark class responsible for security.
 *
 * In general this class should be instantiated by the SparkEnv and most components
 * should access it from that. There are some cases where the SparkEnv hasn't been
 * initialized yet and this class must be instantiated directly.
 *
 * This class implements all of the configuration related to security features described
 * in the "Security" document. Please refer to that document for specific features implemented
 * here.
 */
private[spark] class SecurityManager(
    sparkConf: SparkConf,
    val ioEncryptionKey: Option[Array[Byte]] = None,
    authSecretFileConf: ConfigEntry[Option[String]] = AUTH_SECRET_FILE)
  extends Logging with SecretKeyHolder {

  import SecurityManager._

  // allow all users/groups to have view/modify permissions
  private val WILDCARD_ACL = "*"

  private val authOn = sparkConf.get(NETWORK_AUTH_ENABLED)
  private var aclsOn = sparkConf.get(ACLS_ENABLE)

  // admin acls should be set before view or modify acls
  private var adminAcls: Set[String] = sparkConf.get(ADMIN_ACLS).toSet

  // admin group acls should be set before view or modify group acls
  private var adminAclsGroups: Set[String] = sparkConf.get(ADMIN_ACLS_GROUPS).toSet

  private var viewAcls: Set[String] = _

  private var viewAclsGroups: Set[String] = _

  // list of users who have permission to modify the application. This should
  // apply to both UI and CLI for things like killing the application.
  private var modifyAcls: Set[String] = _

  private var modifyAclsGroups: Set[String] = _

  // always add the current user and SPARK_USER to the viewAcls
  private val defaultAclUsers = Set[String](System.getProperty("user.name", ""),
    Utils.getCurrentUserName())

  setViewAcls(defaultAclUsers, sparkConf.get(UI_VIEW_ACLS))
  setModifyAcls(defaultAclUsers, sparkConf.get(MODIFY_ACLS))

  setViewAclsGroups(sparkConf.get(UI_VIEW_ACLS_GROUPS))
  setModifyAclsGroups(sparkConf.get(MODIFY_ACLS_GROUPS))

  private var secretKey: String = _
  logInfo("SecurityManager: authentication " + (if (authOn) "enabled" else "disabled") +
    "; ui acls " + (if (aclsOn) "enabled" else "disabled") +
    "; users with view permissions: " +
    (if (viewAcls.nonEmpty) viewAcls.mkString(", ") else "EMPTY") +
    "; groups with view permissions: " +
    (if (viewAclsGroups.nonEmpty) viewAclsGroups.mkString(", ") else "EMPTY") +
    "; users with modify permissions: " +
    (if (modifyAcls.nonEmpty) modifyAcls.mkString(", ") else "EMPTY") +
    "; groups with modify permissions: " +
    (if (modifyAclsGroups.nonEmpty) modifyAclsGroups.mkString(", ") else "EMPTY"))

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
  // the default SSL configuration - it will be used by all communication layers unless overwritten
  private val defaultSSLOptions =
    SSLOptions.parse(sparkConf, hadoopConf, "spark.ssl", defaults = None)

  def getSSLOptions(module: String): SSLOptions = {
    val opts =
      SSLOptions.parse(sparkConf, hadoopConf, s"spark.ssl.$module", Some(defaultSSLOptions))
    logDebug(s"Created SSL options for $module: $opts")
    opts
  }

  /**
    * Generates ssl ceritificates for Spark Web UI if Ssl is enabled and
    * certificates are not specified by the user. Otherwise returns
    * sslOptions without any changes.
    */

  def copyManageSslKeysScriptToMapRFsIfNeeded(): Unit = {
    if (isSSLCertGenerationNeededForWebUI(getSSLOptions("ui"))) {
      val certGeneratorName = "manageSSLKeys.sh"
      val username = UserGroupInformation.getCurrentUser.getShortUserName
      val fs = FileSystem.get(SparkHadoopUtil.get.newConfiguration(sparkConf))
      val mfsBaseDir = s"/apps/spark/__$username-spark-internal__/security_keys/"

      val mfsManageSslKeysScript = s"$mfsBaseDir/$certGeneratorName"

      if (fs.exists(new Path(mfsManageSslKeysScript))) {
        return
      } else {
        val mfsBaseDirPath = new Path(mfsBaseDir)
        if (!fs.exists(mfsBaseDirPath)) {
          fs.mkdirs(mfsBaseDirPath)
        }
      }

      def getSparkVersion(sparkBase: String) = {
        val sparkVersionFile = scala.io.Source.fromFile(s"$sparkBase/sparkversion")
        try {
          val sparkVersion = sparkVersionFile.mkString.trim
          sparkVersion
        } catch {
          case e: FileNotFoundException =>
            throw new Exception(s"Failed to generate SSL certificates for spark WebUI: ", e)
        } finally {
          sparkVersionFile.close()
        }
      }

      val maprHomeEnv = System.getenv("MAPR_HOME")
      val maprHome = if (maprHomeEnv == null || maprHomeEnv.isEmpty) "/opt/mapr" else maprHomeEnv
      val sparkBase = s"$maprHome/spark"
      val sparkVersion: String = getSparkVersion(sparkBase)
      val sparkHome = s"$sparkBase/spark-$sparkVersion"
      val manageSslKeysScript = s"$sparkHome/bin/$certGeneratorName"

      fs.copyFromLocalFile(new Path(manageSslKeysScript), new Path(mfsManageSslKeysScript))
    }
  }

  def genSslCertsForWebUIifNeeded(sslOptions: SSLOptions): SSLOptions = {
    if (isSSLCertGenerationNeededForWebUI(sslOptions)) {
      val currentUserHomeDir = System.getProperty("user.home")
      val localBaseDir = s"$currentUserHomeDir/__spark-internal__/security_keys"
      val sslKeyStore = s"$localBaseDir/ssl_keystore"
      val sslKeyStorePass = "mapr123" // todo remove password from source code
      val updatedSslOptions = updateSslOptsWithNewKeystore(sslOptions, sslKeyStore, sslKeyStorePass)

      if (!Files.exists(Paths.get(sslKeyStore))) {
        copyFromMfsOrGenSslCertsForWebUI(localBaseDir, sslKeyStorePass)
      }
      updatedSslOptions
    } else {
      sslOptions
    }
  }

  def isSSLCertGenerationNeededForWebUI(sslOptions: SSLOptions): Boolean = {
    sslOptions.enabled && sslOptions.keyStore.isEmpty
  }

  def copyFromMfsOrGenSslCertsForWebUI(localBaseDir: String, sslKeyStorePass: String) {
    //////////////////// Zookeeper lock utils /////////////////////
    val mfs = FileSystem.get(new Configuration()).asInstanceOf[MapRFileSystem]
    val zkUrl = mfs.getZkConnectString
    val sslZkConfProperty = "tmp.spark.ssl.zookeeper.url"
    val zkPath = "/spark/web-ui-locks"
    val zkLock = s"$zkPath/lock-"

    sparkConf.set(sslZkConfProperty, zkUrl)
    val zk: CuratorFramework = SparkCuratorUtil.newClient(sparkConf, sslZkConfProperty)
    sparkConf.remove(sslZkConfProperty)

    if (zk.checkExists().forPath(zkPath) == null) {
      zk.create().creatingParentsIfNeeded().forPath(zkPath)
    }

    def aquireLock(): String = {
      val lockPath = zk.create().withProtectedEphemeralSequential().forPath(zkLock);
      val lock = new Object()
      lock.synchronized {
        while (true) {
          val nodes = zk.getChildren().usingWatcher(new CuratorWatcher {
            override def process(watchedEvent: WatchedEvent): Unit = {
              lock.synchronized {
                lock.notifyAll()
              }
            }
          }).forPath(zkPath)
          val sortedNodes = nodes.asScala.sorted
          if (lockPath.endsWith(nodes.get(0))) {
            return lockPath
          } else {
            lock.wait()
          }
        }
      }
      lockPath
    }

    def releaseLock(lockPath : String): Unit = {
      zk.delete().forPath(lockPath)
    }
    /////////////////////End of Zookeeper lock utils //////////////////////

    val username = UserGroupInformation.getCurrentUser.getShortUserName
    val mfsBaseDir = s"/apps/spark/__$username-spark-internal__/security_keys"
    val mfsKeyStore = s"$mfsBaseDir/ssl_keystore"
    val fs = FileSystem.get(hadoopConf)

    val f = new File(localBaseDir)
    if (!f.exists()) {
      f.mkdirs()
    }


    if (fs.exists(new Path(mfsKeyStore))) {
      val files = fs.listFiles(new Path(mfsBaseDir), false)
      files.next().getPath
      while(files.hasNext) {
        val f = files.next()
        fs.copyToLocalFile(f.getPath, new Path(localBaseDir))
      }
    } else {
       val lockPath = aquireLock()
      if (! fs.exists(new Path(mfsKeyStore))) {
        genSslCertsForWebUI(localBaseDir, mfsBaseDir, sslKeyStorePass)
      }
      releaseLock(lockPath)
    }
  }

  private def updateSslOptsWithNewKeystore(sslOptions: SSLOptions,
                                           sslKeyStore: String,
                                           sslKeyStorePass: String): SSLOptions = {
    new SSLOptions(
      sslOptions.enabled,
      sslOptions.port,
      Some(new File(sslKeyStore)),
      Some(sslKeyStorePass),
      sslOptions.keyPassword,
      sslOptions.keyStoreType,
      sslOptions.needClientAuth,
      sslOptions.trustStore,
      sslOptions.trustStorePassword,
      sslOptions.trustStoreType,
      sslOptions.protocol,
      sslOptions.enabledAlgorithms)
  }

  private def genSslCertsForWebUI(localBaseDir: String,
                                  mfsBaseDir : String,
                                  sslKeyStorePass: String) {
    val certGeneratorName = "manageSSLKeys.sh"
    val mfsManageSslKeysScript = s"$mfsBaseDir/$certGeneratorName"
    val manageSslKeysScript = s"$localBaseDir/$certGeneratorName"

    val directory = new File(localBaseDir)
    if (!directory.exists()) {
      directory.mkdir()
    }

    val fs = FileSystem.get(hadoopConf)
    fs.copyToLocalFile(new Path(mfsManageSslKeysScript), new Path(localBaseDir))

    val manageSslKeysLocalFile = new File(manageSslKeysScript)
    manageSslKeysLocalFile.setExecutable(true)

    val res = s"$manageSslKeysScript $sslKeyStorePass".!
    if (res != 0) {
      throw new Exception(s"Failed to generate SSL certificates for spark WebUI")
    }
  }

  /**
   * Split a comma separated String, filter out any empty items, and return a Set of strings
   */
  private def stringToSet(list: String): Set[String] = {
    list.split(',').map(_.trim).filter(!_.isEmpty).toSet
  }

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setViewAcls(defaultUsers: Set[String], allowedUsers: Seq[String]): Unit = {
    viewAcls = adminAcls ++ defaultUsers ++ allowedUsers
    logInfo("Changing view acls to: " + viewAcls.mkString(","))
  }

  def setViewAcls(defaultUser: String, allowedUsers: Seq[String]): Unit = {
    setViewAcls(Set[String](defaultUser), allowedUsers)
  }

  /**
   * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
   * acls groups you should also set the view and modify acls groups again to pick up the changes.
   */
  def setViewAclsGroups(allowedUserGroups: Seq[String]): Unit = {
    viewAclsGroups = adminAclsGroups ++ allowedUserGroups
    logInfo("Changing view acls groups to: " + viewAclsGroups.mkString(","))
  }

  /**
   * Checking the existence of "*" is necessary as YARN can't recognize the "*" in "defaultuser,*"
   */
  def getViewAcls: String = {
    if (viewAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAcls.mkString(",")
    }
  }

  def getViewAclsGroups: String = {
    if (viewAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAclsGroups.mkString(",")
    }
  }

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setModifyAcls(defaultUsers: Set[String], allowedUsers: Seq[String]): Unit = {
    modifyAcls = adminAcls ++ defaultUsers ++ allowedUsers
    logInfo("Changing modify acls to: " + modifyAcls.mkString(","))
  }

  /**
   * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
   * acls groups you should also set the view and modify acls groups again to pick up the changes.
   */
  def setModifyAclsGroups(allowedUserGroups: Seq[String]): Unit = {
    modifyAclsGroups = adminAclsGroups ++ allowedUserGroups
    logInfo("Changing modify acls groups to: " + modifyAclsGroups.mkString(","))
  }

  /**
   * Checking the existence of "*" is necessary as YARN can't recognize the "*" in "defaultuser,*"
   */
  def getModifyAcls: String = {
    if (modifyAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAcls.mkString(",")
    }
  }

  def getModifyAclsGroups: String = {
    if (modifyAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAclsGroups.mkString(",")
    }
  }

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setAdminAcls(adminUsers: Seq[String]): Unit = {
    adminAcls = adminUsers.toSet
    logInfo("Changing admin acls to: " + adminAcls.mkString(","))
  }

  /**
   * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
   * acls groups you should also set the view and modify acls groups again to pick up the changes.
   */
  def setAdminAclsGroups(adminUserGroups: Seq[String]): Unit = {
    adminAclsGroups = adminUserGroups.toSet
    logInfo("Changing admin acls groups to: " + adminAclsGroups.mkString(","))
  }

  def setAcls(aclSetting: Boolean): Unit = {
    aclsOn = aclSetting
    logInfo("Changing acls enabled to: " + aclsOn)
  }

  def getIOEncryptionKey(): Option[Array[Byte]] = ioEncryptionKey

  /**
   * Check to see if Acls for the UI are enabled
   * @return true if UI authentication is enabled, otherwise false
   */
  def aclsEnabled(): Boolean = aclsOn

  /**
   * Checks whether the given user is an admin. This gives the user both view and
   * modify permissions, and also allows the user to impersonate other users when
   * making UI requests.
   */
  def checkAdminPermissions(user: String): Boolean = {
    isUserInACL(user, adminAcls, adminAclsGroups)
  }

  /**
   * Checks the given user against the view acl and groups list to see if they have
   * authorization to view the UI. If the UI acls are disabled
   * via spark.acls.enable, all users have view access. If the user is null
   * it is assumed authentication is off and all users have access. Also if any one of the
   * UI acls or groups specify the WILDCARD(*) then all users have view access.
   *
   * @param user to see if is authorized
   * @return true is the user has permission, otherwise false
   */
  def checkUIViewPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " viewAcls=" +
      viewAcls.mkString(",") + " viewAclsGroups=" + viewAclsGroups.mkString(","))
    isUserInACL(user, viewAcls, viewAclsGroups)
  }

  /**
   * Checks the given user against the modify acl and groups list to see if they have
   * authorization to modify the application. If the modify acls are disabled
   * via spark.acls.enable, all users have modify access. If the user is null
   * it is assumed authentication isn't turned on and all users have access. Also if any one
   * of the modify acls or groups specify the WILDCARD(*) then all users have modify access.
   *
   * @param user to see if is authorized
   * @return true is the user has permission, otherwise false
   */
  def checkModifyPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " modifyAcls=" +
      modifyAcls.mkString(",") + " modifyAclsGroups=" + modifyAclsGroups.mkString(","))
    isUserInACL(user, modifyAcls, modifyAclsGroups)
  }

  /**
   * Check to see if authentication for the Spark communication protocols is enabled
   * @return true if authentication is enabled, otherwise false
   */
  def isAuthenticationEnabled(): Boolean = authOn

  /**
   * Checks whether network encryption should be enabled.
   * @return Whether to enable encryption when connecting to services that support it.
   */
  def isEncryptionEnabled(): Boolean = {
    sparkConf.get(Network.NETWORK_CRYPTO_ENABLED) || sparkConf.get(SASL_ENCRYPTION_ENABLED)
  }

  /**
   * Gets the user used for authenticating SASL connections.
   * For now use a single hardcoded user.
   * @return the SASL user as a String
   */
  def getSaslUser(): String = "sparkSaslUser"

  /**
   * Gets the secret key.
   * @return the secret key as a String if authentication is enabled, otherwise returns null
   */
  def getSecretKey(): String = {
    if (isAuthenticationEnabled) {
      val creds = UserGroupInformation.getCurrentUser().getCredentials()
      Option(creds.getSecretKey(SECRET_LOOKUP_KEY))
        .map { bytes => new String(bytes, UTF_8) }
        // Secret key may not be found in current UGI's credentials.
        // This happens when UGI is refreshed in the driver side by UGI's loginFromKeytab but not
        // copy secret key from original UGI to the new one. This exists in ThriftServer's Hive
        // logic. So as a workaround, storing secret key in a local variable to make it visible
        // in different context.
        .orElse(Option(secretKey))
        .orElse(Option(sparkConf.getenv(ENV_AUTH_SECRET)))
        .orElse(sparkConf.getOption(SPARK_AUTH_SECRET_CONF))
        .orElse(secretKeyFromFile())
        .getOrElse {
          throw new IllegalArgumentException(
            s"A secret key must be specified via the $SPARK_AUTH_SECRET_CONF config")
        }
    } else {
      null
    }
  }

  /**
   * Initialize the authentication secret.
   *
   * If authentication is disabled, do nothing.
   *
   * In YARN and local mode, generate a new secret and store it in the current user's credentials.
   *
   * In other modes, assert that the auth secret is set in the configuration.
   */
  def initializeAuth(): Unit = {

    if (!sparkConf.get(NETWORK_AUTH_ENABLED)) {
      return
    }

    // TODO: this really should be abstracted somewhere else.
    val master = sparkConf.get(SparkLauncher.SPARK_MASTER, "")
    val storeInUgi = master match {
      case "yarn" | "local" | LOCAL_N_REGEX(_) | LOCAL_N_FAILURES_REGEX(_, _) =>
        true

      case KUBERNETES_REGEX(_) =>
        // Don't propagate the secret through the user's credentials in kubernetes. That conflicts
        // with the way k8s handles propagation of delegation tokens.
        false

      case _ =>
        require(sparkConf.contains(SPARK_AUTH_SECRET_CONF),
          s"A secret key must be specified via the $SPARK_AUTH_SECRET_CONF config.")
        return
    }

    if (sparkConf.get(AUTH_SECRET_FILE_DRIVER).isDefined !=
        sparkConf.get(AUTH_SECRET_FILE_EXECUTOR).isDefined) {
      throw new IllegalArgumentException(
        "Invalid secret configuration: Secret files must be specified for both the driver and the" +
          " executors, not only one or the other.")
    }

    secretKey = secretKeyFromFile().getOrElse(Utils.createSecret(sparkConf))

    if (storeInUgi) {
      val creds = new Credentials()
      creds.addSecretKey(SECRET_LOOKUP_KEY, secretKey.getBytes(UTF_8))
      UserGroupInformation.getCurrentUser().addCredentials(creds)
    }
  }

  private def secretKeyFromFile(): Option[String] = {
    sparkConf.get(authSecretFileConf).flatMap { secretFilePath =>
      sparkConf.getOption(SparkLauncher.SPARK_MASTER).map {
        case SparkMasterRegex.KUBERNETES_REGEX(_) =>
          val secretFile = new File(secretFilePath)
          require(secretFile.isFile, s"No file found containing the secret key at $secretFilePath.")
          val base64Key = Base64.getEncoder.encodeToString(Files.readAllBytes(secretFile.toPath))
          require(!base64Key.isEmpty, s"Secret key from file located at $secretFilePath is empty.")
          base64Key
        case _ =>
          throw new IllegalArgumentException(
            "Secret keys provided via files is only allowed in Kubernetes mode.")
      }
    }
  }

  private def isUserInACL(
      user: String,
      aclUsers: Set[String],
      aclGroups: Set[String]): Boolean = {
    if (user == null ||
        !aclsEnabled ||
        aclUsers.contains(WILDCARD_ACL) ||
        aclUsers.contains(user) ||
        aclGroups.contains(WILDCARD_ACL)) {
      true
    } else {
      val userGroups = Utils.getCurrentUserGroups(sparkConf, user)
      logDebug(s"user $user is in groups ${userGroups.mkString(",")}")
      aclGroups.exists(userGroups.contains(_))
    }
  }

  // Default SecurityManager only has a single secret key, so ignore appId.
  override def getSaslUser(appId: String): String = getSaslUser()
  override def getSecretKey(appId: String): String = getSecretKey()
}

private[spark] object SecurityManager {

  val SPARK_AUTH_CONF = NETWORK_AUTH_ENABLED.key
  val SPARK_AUTH_SECRET_CONF = AUTH_SECRET.key
  // This is used to set auth secret to an executor's env variable. It should have the same
  // value as SPARK_AUTH_SECRET_CONF set in SparkConf
  val ENV_AUTH_SECRET = "_SPARK_AUTH_SECRET"

  // key used to store the spark secret in the Hadoop UGI
  val SECRET_LOOKUP_KEY = new Text("sparkCookie")
}
