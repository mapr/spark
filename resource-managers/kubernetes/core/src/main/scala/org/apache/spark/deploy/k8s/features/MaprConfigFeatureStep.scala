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
    applyLdapCM(podBuilder, containerBuilder)
    applySSSDSecret(podBuilder, containerBuilder)
    applySSHSecret(podBuilder, containerBuilder)
    applyClientSecret(podBuilder, containerBuilder)

    SparkPod(podBuilder.build(), containerBuilder.build())
  }

  private def applyLdapCM(podBuilder: PodBuilder, containerBuilder: ContainerBuilder) = {
    val cmName = "ldapclient-cm"
    val cmVolumeName = "ldap-cm"

    podBuilder.editOrNewSpec()
      .addNewVolume()
      .withName(cmVolumeName)
      .withNewConfigMap()
      .withName(cmName)
      .withOptional(true)
      .withDefaultMode(420)
      .endConfigMap()
      .endVolume()
      .endSpec()

    containerBuilder.addNewVolumeMount()
      .withName(cmVolumeName)
      .withMountPath("/opt/mapr/kubernetes/ldap-cm")
      .endVolumeMount()
  }

  private def applySSSDSecret(podBuilder: PodBuilder, containerBuilder: ContainerBuilder) = {
    val secretName = "sssd"
    val volumeName = "sssd-secrets"

    podBuilder.editOrNewSpec()
      .addNewVolume()
      .withName(volumeName)
      .withNewSecret()
      .withSecretName(secretName)
      .withDefaultMode(420)
      .withOptional(true)
      .endSecret()
      .endVolume()
      .endSpec()

    containerBuilder.addNewVolumeMount()
      .withName(volumeName)
      .withMountPath("/opt/mapr/kubernetes/sssd-secrets")
      .endVolumeMount()
  }

  private def applySSHSecret(podBuilder: PodBuilder, containerBuilder: ContainerBuilder) = {
    val secretName = "ssh"
    val volumeName = "ssh-secrets"

    podBuilder.editOrNewSpec()
      .addNewVolume()
      .withName(volumeName)
      .withNewSecret()
      .withSecretName(secretName)
      .withDefaultMode(420)
      .withOptional(true)
      .endSecret()
      .endVolume()
      .endSpec()

    containerBuilder.addNewVolumeMount()
      .withName(volumeName)
      .withMountPath("/opt/mapr/kubernetes/ssh-secrets")
      .endVolumeMount()
  }

  private def applyClientSecret(podBuilder: PodBuilder, containerBuilder: ContainerBuilder) = {
    val secretName = "client"
    val volumeName = "client-secrets"

    podBuilder.editOrNewSpec()
      .addNewVolume()
      .withName(volumeName)
      .withNewSecret()
      .withSecretName(secretName)
      .withDefaultMode(420)
      .withOptional(true)
      .endSecret()
      .endVolume()
      .endSpec()

    containerBuilder.addNewVolumeMount()
      .withName(volumeName)
      .withMountPath("/opt/mapr/kubernetes/client-secrets")
      .endVolumeMount()
  }

  private def applyUserSecret(podBuilder: PodBuilder, containerBuilder: ContainerBuilder): Unit = {
    val userSecretName = sparkConf.get(MAPR_USER_SECRET)
    val userSecretVolumeName = s"$userSecretName-volume"

    if (userSecretName.isEmpty) {
      return
    }

    podBuilder.editOrNewSpec()
      .addNewVolume()
        .withName(userSecretVolumeName)
        .withNewSecret()
          .withSecretName(userSecretName.get)
        .endSecret()
      .endVolume()
      .endSpec()

    containerBuilder
      .addNewEnv()
        .withName(ENV_MAPR_TICKETFILE_LOCATION)
        .withValue(MAPR_USER_TICKET_MOUNT_PATH)
      .endEnv()
      .addNewEnv()
        .withName(ENV_MAPR_USERSECRET_MOUNT_PATH)
        .withValue(MAPR_USER_SECRET_MOUNT_PATH)
      .endEnv()
      .addNewVolumeMount()
        .withName(userSecretVolumeName)
        .withMountPath(MAPR_USER_SECRET_MOUNT_PATH)
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
          .withOptional(true)
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
          .withOptional(true)
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
