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

package org.apache.spark.streaming.kafka.producer.utils

import java.util.{UUID, Properties}
import java.util.concurrent.TimeUnit

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkFunSuite
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait KafkaServerUtil extends SparkFunSuite with BeforeAndAfterAll {
  var zookeeper: EmbeddedZookeeper = null

  var kafka: EmbeddedKafka = null

  var zkUtils: ZkUtils = null

  val ZkConnTimeout = 10000

  val ServiceStartTimeout = 1

  val TopicCreateTimeout = 1

  private val consumerProperties = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getCanonicalName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getCanonicalName,
    ConsumerConfig.GROUP_ID_CONFIG -> "test",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
  )

  override protected def beforeAll(): Unit = {
    zookeeper = new EmbeddedZookeeper()
    zookeeper.setup()
    zookeeper.start()
    TimeUnit.SECONDS.sleep(ServiceStartTimeout)
    require(zookeeper.server.isRunning, "Zookeeper should be running on that step")
    kafka = new EmbeddedKafka(zookeeper.zkConnect())
    kafka.setup()
    kafka.start()
    TimeUnit.SECONDS.sleep(ServiceStartTimeout)
    val zkClient = ZkUtils.createZkClient(zookeeper.zkConnect(),
      sessionTimeout = ZkConnTimeout, connectionTimeout = ZkConnTimeout)
    zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
  }

  override protected def afterAll(): Unit = {
    Option(kafka).foreach(_.shutdown())
    Option(zookeeper).foreach(_.shutdown())
    super.afterAll()
  }

  def createTestTopic(topicId: String, nPartitions: Int = 1, replicationFactor: Int = 1): String = {
    val topic = topicId + UUID.randomUUID()
    AdminUtils.createTopic(zkUtils, topic, nPartitions, replicationFactor)
    TimeUnit.SECONDS.sleep(TopicCreateTimeout)
    assert(AdminUtils.topicExists(zkUtils, topic), s"Failed to create topic=$topic")
    topic
  }

  def consumerForTopic(topicId: String): KafkaConsumer[String, String] = {
    val props = new Properties()
    consumerProperties.foreach(prop => props.put(prop._1, prop._2))
    new KafkaConsumer[String, String](props)
  }
}
