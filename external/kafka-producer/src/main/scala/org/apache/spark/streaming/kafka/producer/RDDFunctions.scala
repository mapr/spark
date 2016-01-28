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

import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDFunctions[T: ClassTag](rdd: RDD[T]) extends Serializable {
  def sendToKafka[S <: Serializer[T] : ClassTag](
    topic: String,
    conf: ProducerConf): Unit = {
    rdd.sparkContext.runJob(
      rdd,
      new KafkaRDDWriter[Array[Byte], T, ByteArraySerializer, S](topic, conf).sendV _)
  }
}

class PairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {
  def sendToKafka[
    KS <: Serializer[K] : ClassTag,
    VS <: Serializer[V] : ClassTag
  ](topic: String, conf: ProducerConf): Unit = {
    rdd.sparkContext.runJob(
      rdd,
      new KafkaRDDWriter[K, V, KS, VS](topic, conf).sendKV _)
  }
}
