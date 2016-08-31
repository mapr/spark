package org.apache.spark.deploy.k8s.features

import scala.collection.JavaConverters._
import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, HasMetadata, PodBuilder, VolumeBuilder}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesRoleSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

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

    val clusterConfMap = sparkConf.get(MAPR_CLUSTER_CONFIGMAP).toString
    val userSecret = sparkConf.get(MAPR_USER_SECRET).toString
    val userSecretVolumeName = s"$userSecret-volume"
    val userSecretMountPath = "/tmp/maprticket"
    val ticketFileLocation = s"$userSecretMountPath/${sparkConf.get(MAPR_TICKET_SECRET_KEY)}"

    val maprPod = new PodBuilder(pod.pod)
      .editOrNewSpec()
      .addToVolumes(
        new VolumeBuilder()
          .withName(userSecretVolumeName)
          .withNewSecret()
            .withSecretName(userSecret)
          .endSecret()
          .build())
      .endSpec()
      .build()

    val maprContainer = new ContainerBuilder(pod.container)
      .addAllToEnv(clusterEnvs.asJava)
      .addNewEnv()
        .withName(MAPR_TICKETFILE_LOCATION)
        .withValue(ticketFileLocation)
      .endEnv()
      .addNewVolumeMount()
        .withName(userSecretVolumeName)
        .withMountPath(userSecretMountPath)
      .endVolumeMount()
      .addNewEnvFrom()
        .withNewConfigMapRef()
          .withName(clusterConfMap)
          .endConfigMapRef()
        .endEnvFrom()
      .addNewEnvFrom()
        .withNewSecretRef()
          .withName(userSecret)
          .endSecretRef()
        .endEnvFrom()
      .build()

    SparkPod(maprPod, maprContainer)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
