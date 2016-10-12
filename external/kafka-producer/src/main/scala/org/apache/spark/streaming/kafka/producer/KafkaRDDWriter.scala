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

import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.Serializer

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging

class KafkaRDDWriter[
  K: ClassTag,
  V: ClassTag,
  KS <: Serializer[K] : ClassTag,
  VS <: Serializer[V] : ClassTag](
    val topic: String,
    val producerConf: ProducerConf) extends Serializable with Logging {

  private var producer: KafkaProducer[K, V] = null

  private val callback: KafkaCallback = new KafkaCallback()

  private def initializeProducer(producerConf: ProducerConf): KafkaProducer[K, V] = {
    val conf = producerConf
      .withKeySerializer(classTag[KS].runtimeClass.getName)
      .withValueSerializer(classTag[VS].runtimeClass.getName)
    Try(new KafkaProducer[K, V](conf.asJMap())) match {
      case Success(_producer) => _producer
      case Failure(kafkaException) =>
        throw new SparkException("Failed to instantiate Kafka producer due to: ", kafkaException)
    }
  }

  def sendV(taskContext: TaskContext, records: Iterator[V]): Unit = {
    sendRecords[V](taskContext, records, toValueRecord)
  }

  def sendKV(taskContext: TaskContext, records: Iterator[(K, V)]): Unit = {
    sendRecords[(K, V)](taskContext, records, kv => toKeyValueRecord(kv._1, kv._2))
  }

  private def sendRecords[R](
      taskContext: TaskContext,
      records: Iterator[R],
      recordMapper: R => ProducerRecord[K, V]): Unit = {
    if (records.isEmpty) {
      logDebug(s"No data to send: rdd-partition=${taskContext.partitionId()}")
      return
    }
    try {
      producer = initializeProducer(producerConf)

      taskContext.addTaskCompletionListener((taskContext: TaskContext) => {
        log.debug(s"Task completed: topic=$topic, rdd-partition=${taskContext.partitionId()}." +
          s" Closing producer")
        Option(producer).foreach(_.close())
      })

      log.debug(s"Sending data: topic=$topic, rdd-partition=${taskContext.partitionId()}")
      records.map(recordMapper).foreach(producer.send(_, callback))
      producer.flush()
    } catch {
      case kex: KafkaException => throw new SparkException(kex.getMessage)
    } finally {
      Option(producer).foreach(_.close())
    }
  }

  private def toValueRecord(value: V): ProducerRecord[K, V] = {
    new ProducerRecord[K, V](topic, value)
  }

  private def toKeyValueRecord(key: K, value: V): ProducerRecord[K, V] = {
    new ProducerRecord[K, V](topic, key, value)
  }
}

class KafkaCallback extends Callback with Serializable {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      throw new SparkException("Failed to send data due to: ", exception)
    }
  }
}
