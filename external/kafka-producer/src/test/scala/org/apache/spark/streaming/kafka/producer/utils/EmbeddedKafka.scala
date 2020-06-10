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

import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

import scala.collection.JavaConverters._
import scala.util.Try

sealed trait EmbeddedService {
  def setup(): Unit

  def start(): Unit

  def shutdown(): Unit
}

private[spark] final class EmbeddedKafka(
  zkConnect: String = "localhost:2181",
  kafkaProps: Option[Properties] = None) extends EmbeddedService {

  lazy private val kafkaConfig: KafkaConfig = {
    val conf: Map[String, String] = Map(
      "broker.id" -> "1",
      "zookeeper.connect" -> zkConnect,
      "host.name" -> "localhost",
      "offsets.topic.replication.factor" -> "1",
      "log.dir" -> logDir.toString)

    val props = kafkaProps.getOrElse(new Properties())
    // props.putAll(conf.asJava)
    // known issue https://github.com/scala/bug/issues/10418 (for Scala-2.12+ and JDK 9+)
    conf.asJava.forEach((k, v) => props.put(k, v))
    new KafkaConfig(props)
  }
  private val logDir = Files.createTempDirectory("kafka-log")
  var kafka: KafkaServer = _

  def start(): Unit = {
    kafka.startup()
  }

  def shutdown(): Unit = {
    Try(kafka.shutdown())
    Try(Utils.deleteRecursively(logDir.toFile))
  }

  override def setup(): Unit = {
    kafka = new KafkaServer(kafkaConfig)
  }
}

private[spark] final class EmbeddedZookeeper(
  zkHost: String = "localhost",
  zkPort: Int = 2181) extends EmbeddedService {

  private val ConnTimeout: Int = 6000
  private val snapDir = Files.createTempDirectory("zk-snapshot")
  private val logDir = Files.createTempDirectory("zk-log")
  private val tickTime = 500
  var server: ZooKeeperServer = _
  private var factory: NIOServerCnxnFactory = _

  def zkClient(): ZkClient = new ZkClient(zkConnect(), ConnTimeout)

  def zkConnect(): String = s"${actualHost()}:${actualPort()}"

  private def actualHost(): String = factory.getLocalAddress.getHostName

  private def actualPort(): Int = factory.getLocalPort

  def start(): Unit = {
    factory.startup(server)
  }

  def shutdown(): Unit = {
    Try(server.shutdown())
    Try(factory.shutdown())
    Try(cleanup())
  }

  private def cleanup(): Unit = {
    Utils.deleteAllRecursively(snapDir.toFile, logDir.toFile)
  }

  override def setup(): Unit = {
    server = new ZooKeeperServer(snapDir.toFile, logDir.toFile, tickTime)
    factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(zkHost, zkPort), 16)
  }
}
