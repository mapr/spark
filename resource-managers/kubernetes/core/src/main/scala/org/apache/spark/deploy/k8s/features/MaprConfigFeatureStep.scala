package org.apache.spark.deploy.k8s.features

import scala.collection.JavaConverters._
import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, HasMetadata}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesRoleSpecificConf, SparkPod}
import org.apache.spark.util.Utils

private[spark] class MaprConfigFeatureStep(
    conf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {

  val sparkConf: SparkConf = conf.sparkConf

  override def configurePod(pod: SparkPod): SparkPod = {
    val clusterEnvs = sparkConf.getAllWithPrefix(KUBERNETES_CLUSTER_ENV_KEY).toSeq
      .map { case (name, value) =>
        new EnvVarBuilder()
          .withName(name)
          .withValue(value)
          .build()
      }

    val maprTicketSecret =
      s"$KUBERNETES_DRIVER_SECRETS_PREFIX${sparkConf.get(MAPR_TICKET_SECRET_PREFIX)}"

    val maprTicketEnv = sparkConf
      .getAllWithPrefix(maprTicketSecret).toSeq
      .map { case (_, value) =>
        new EnvVarBuilder()
          .withName(MAPR_TICKETFILE_LOCATION)
          .withValue(value + s"/${sparkConf.get(MAPR_TICKET_SECRET_KEY)}")
          .build()
      }

    val maprSslSecret =
      s"$KUBERNETES_DRIVER_SECRETS_PREFIX${sparkConf.get(MAPR_SSL_SECRET_PREFIX)}"

    val maprSslEnv = sparkConf
      .getAllWithPrefix(maprSslSecret).toSeq
      .map { case (_, value) =>
        new EnvVarBuilder()
          .withName(MAPR_SSL_LOCATION)
          .withValue(value)
          .build()
      }

    val clusterConfMap = sparkConf.get(MAPR_CLUSTER_CONFIGMAP).toString
    val clusterUserSecrets = sparkConf.get(MAPR_CLUSTER_USER_SECRETS).toString

    val (username, userGroups) = sparkConf.get(CONTAINER_USER_NAME) match {
      case Some(user) => (user, Set(user))
      case None =>
        val currentUser = Utils.getCurrentUserName()
        val currentUserGroups = Utils.getCurrentUserGroups(sparkConf, currentUser)
        (currentUser, currentUserGroups)
    }

    val (userId, userGroupsIds) = sparkConf.get(CONTAINER_USER_ID) match {
      case Some(id) => (id, Set(id))
      case None =>
        val currentUser = Utils.getCurrentUserId()
        val currentUserGroups = Utils.getCurrentUserGroupsIds(sparkConf, currentUser)
        (currentUser, currentUserGroups)
    }

    if (userId.isEmpty || userGroupsIds.isEmpty) {
      throw new RuntimeException(s"Error getting uid/gid for user=$username")
    }

    val container = new ContainerBuilder(pod.container)
      .addAllToEnv(clusterEnvs.asJava)
      .addAllToEnv(maprTicketEnv.asJava)
      .addAllToEnv(maprSslEnv.asJava)
      .addNewEnv()
        .withName(CURRENT_USER)
        .withValue(username)
        .endEnv()
      .addNewEnv()
        .withName(USER_GROUPS)
        .withValue(userGroups.mkString(" "))
        .endEnv()
      .addNewEnv()
        .withName(CURRENT_USER_ID)
        .withValue(userId)
        .endEnv()
      .addNewEnv()
        .withName(USER_GROUPS_IDS)
        .withValue(userGroupsIds.mkString(" "))
        .endEnv()
      .addNewEnvFrom()
        .withNewConfigMapRef()
          .withName(clusterConfMap)
          .endConfigMapRef()
        .endEnvFrom()
      .addNewEnvFrom()
        .withNewSecretRef()
          .withName(clusterUserSecrets)
          .endSecretRef()
        .endEnvFrom()
      .build()

    SparkPod(pod.pod, container)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
