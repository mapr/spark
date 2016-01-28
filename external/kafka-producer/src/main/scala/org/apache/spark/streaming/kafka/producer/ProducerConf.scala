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

package org.apache.spark.streaming.kafka.producer

import scala.collection.JavaConverters._

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.SparkConf

case class ProducerConf(
    bootstrapServers: List[String] = List("localhost:9092"),
    acks: String = "1",
    bufferMemory: Long = 33554432L,
    compressionType: Option[String] = None,
    retries: Int = 0,
    batchSize: Int = 16384,
    linger: Long = 0,
    others: Map[String, String] = Map.empty[String, String]) {

  private var keySerializer: Option[String] = null

  private var valueSerializer: Option[String] = null

  def asJMap(): java.util.Map[String, Object] = {
    val kafkaParams = Map[String, AnyRef](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers.mkString(","),
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> keySerializer.getOrElse(
        ProducerConf.ByteArraySerializer),
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> valueSerializer.getOrElse(
        ProducerConf.StringSerializer),
      ProducerConfig.ACKS_CONFIG -> acks,
      ProducerConfig.BUFFER_MEMORY_CONFIG -> bufferMemory.toString,
      ProducerConfig.COMPRESSION_TYPE_CONFIG -> compressionType.getOrElse("none"),
      ProducerConfig.RETRIES_CONFIG -> retries.toString,
      ProducerConfig.BATCH_SIZE_CONFIG -> batchSize.toString,
      ProducerConfig.LINGER_MS_CONFIG -> linger.toString
    ) ++ others

    kafkaParams.asJava
  }

  def withKeySerializer(serializer: String): ProducerConf = {
    this.keySerializer = Some(serializer)
    this
  }

  def withValueSerializer(serializer: String): ProducerConf = {
    this.valueSerializer = Some(serializer)
    this
  }
}

object ProducerConf {

  private val Prefix = "spark.streaming.kafka"

  val ByteArraySerializer = "org.apache.kafka.common.serialization.ByteArraySerializer"

  val StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"

  def apply(sparkConf: SparkConf): ProducerConf = {
    val configsMap = sparkConf.getAll
      .filter(isKafkaStreamingConf)
      .map(normalizeConfigName)
      .toMap

    val bootstrapServers: List[String] = configsMap.getOrElse(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
    ).split(",").toList
    val acks: String = configsMap.getOrElse(
      ProducerConfig.ACKS_CONFIG, "1"
    )
    val bufferMemory: Long = configsMap.getOrElse(
      ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"
    ).toLong
    val compressionType: Option[String] = configsMap.get(
      ProducerConfig.COMPRESSION_TYPE_CONFIG
    )
    val retries: Int = configsMap.getOrElse(
      ProducerConfig.RETRIES_CONFIG, "0"
    ).toInt
    val batchSize: Int = configsMap.getOrElse(
      ProducerConfig.BATCH_SIZE_CONFIG, "16384"
    ).toInt
    val linger: Long = configsMap.getOrElse(
      ProducerConfig.LINGER_MS_CONFIG, "0"
    ).toLong

    val others = configsMap - (
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      ProducerConfig.ACKS_CONFIG,
      ProducerConfig.BUFFER_MEMORY_CONFIG,
      ProducerConfig.COMPRESSION_TYPE_CONFIG,
      ProducerConfig.RETRIES_CONFIG,
      ProducerConfig.BATCH_SIZE_CONFIG,
      ProducerConfig.LINGER_MS_CONFIG
    )

    new ProducerConf(
      bootstrapServers = bootstrapServers,
      acks = acks,
      bufferMemory = bufferMemory,
      compressionType = compressionType,
      retries = retries,
      batchSize = batchSize,
      linger = linger,
      others = others)
  }

  private def isKafkaStreamingConf(conf: (String, String)): Boolean = {
    conf._1.startsWith(Prefix)
  }

  private def normalizeConfigName(conf: (String, String)): (String, String) = {
    val (key, value) = conf
    key.split(s"$Prefix.").lastOption match {
      case Some(newName) => newName -> value
      case _ => key -> value
    }
  }
}