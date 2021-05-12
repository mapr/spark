package org.apache.spark.deploy.k8s.features

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, HasMetadata, PodBuilder}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

private[spark] class MaprConfigFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  val sparkConf: SparkConf = conf.sparkConf

  override def configurePod(pod: SparkPod): SparkPod = {
    val podBuilder = new PodBuilder(pod.pod)
    val containerBuilder = new ContainerBuilder(pod.container)

    applyUserSecret(podBuilder, containerBuilder)
    applyMetricsTicket(podBuilder, containerBuilder)
    applyClusterConfigMap(podBuilder, containerBuilder)
    addClusterEnvs(podBuilder, containerBuilder)

    SparkPod(podBuilder.build(), containerBuilder.build())
  }

  private def applyUserSecret(podBuilder: PodBuilder, containerBuilder: ContainerBuilder) = {
    val userSecretName = sparkConf.get(MAPR_USER_SECRET).toString
    val userSecretVolumeName = s"$userSecretName-volume"

    podBuilder.editOrNewSpec()
      .addNewVolume()
        .withName(userSecretVolumeName)
        .withNewSecret()
          .withSecretName(userSecretName)
        .endSecret()
      .endVolume()
      .endSpec()

    containerBuilder
      .addNewEnv()
        .withName(ENV_MAPR_TICKETFILE_LOCATION)
        .withValue(MAPR_USER_TICKET_MOUNT_PATH)
      .endEnv()
      .addNewEnvFrom()
        .withNewSecretRef()
          .withName(userSecretName)
        .endSecretRef()
      .endEnvFrom()
      .addNewVolumeMount()
        .withName(userSecretVolumeName)
        .withMountPath(MAPR_USER_TICKET_MOUNT_PATH)
        .withSubPath(MAPR_USER_TICKET_SUBPATH)
      .endVolumeMount()
  }

  private def applyMetricsTicket(podBuilder: PodBuilder, containerBuilder: ContainerBuilder) = {
    val serverSecretName = MAPR_SERVER_SECRET
    val serverSecretVolume = s"$serverSecretName-volume"

    podBuilder.editOrNewSpec()
      .addNewVolume()
        .withName(serverSecretVolume)
        .withNewSecret()
          .withSecretName(serverSecretName)
        .endSecret()
      .endVolume()
      .endSpec()

    containerBuilder
      .addNewEnv()
        .withName(ENV_MAPR_METRICSFILE_LOCATION)
        .withValue(MAPR_METRICS_TICKET_MOUNT_PATH)
      .endEnv()
      .addNewVolumeMount()
        .withName(serverSecretVolume)
        .withMountPath(MAPR_METRICS_TICKET_MOUNT_PATH)
        .withSubPath(MAPR_METRICS_TICKET_SUBPATH)
      .endVolumeMount()
  }

  private def applyClusterConfigMap(podBuilder: PodBuilder, containerBuilder: ContainerBuilder) = {
    val clusterConfMap = sparkConf.get(MAPR_CLUSTER_CONFIGMAP).toString

    containerBuilder
      .addNewEnvFrom()
        .withNewConfigMapRef()
          .withName(clusterConfMap)
        .endConfigMapRef()
      .endEnvFrom()
  }

  private def addClusterEnvs(podBuilder: PodBuilder, containerBuilder: ContainerBuilder) = {
    val clusterEnvs = sparkConf.getAllWithPrefix(KUBERNETES_CLUSTER_ENV_KEY).toSeq
      .map { case (name, value) =>
        new EnvVarBuilder()
          .withName(name)
          .withValue(value)
          .build()
      }

    containerBuilder
      .addAllToEnv(clusterEnvs.asJava)
      .build()
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
