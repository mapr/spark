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

import scala.language.implicitConversions

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class RDDFunctions[T](rdd: RDD[T]) {
  def sendToKafka(topic: String, conf: ProducerConf): Unit = {
    rdd.foreachPartition(iter => {
      val producer = new KafkaProducer[String, T](conf.asJMap())
      iter.foreach { item =>
        producer.send(new ProducerRecord[String, T](topic, item))
      }
    })
  }
}

class PairRDDFunctions[K, V](rdd: RDD[(K, V)]) {
  def sendToKafka(topic: String, conf: ProducerConf): Unit = {
    rdd.foreachPartition(iter => {
      val producer = new KafkaProducer[K, V](conf.asJMap())
      iter.foreach { item =>
        producer.send(new ProducerRecord[K, V](topic, item._1, item._2))
      }
    })
  }
}