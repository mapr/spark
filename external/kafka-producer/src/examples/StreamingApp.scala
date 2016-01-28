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

import java.util.{Map => JMap}

import org.apache.kafka.common.serialization.Serializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class ItemJsonSerializer extends Serializer[Item] {
  override def configure(configs: JMap[String, _], isKey: Boolean): Unit = { /* NOP */ }

  override def serialize(topic: String, data: Item): Array[Byte] = data.toString.getBytes

  override def close(): Unit = { /* NOP */ }
}

case class Item(id: Int, value: Int) {
  override def toString: String = s"""{"id":"$id","value":"$value"}"""
}

object StreamingApp extends App {
  import org.apache.spark.streaming.kafka.producer._

  val topic = "stream.kafka"
  val numMessages = 10
  val kafkaBrokers = List("localhost:9092")
  val batchTime = Seconds(2)

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .setAppName(getClass.getCanonicalName)
  val ssc = new StreamingContext(sparkConf, batchTime)

  val producerConf = new ProducerConf(
    bootstrapServers = kafkaBrokers)

  val items = (0 until numMessages).map(i => Item(i, i))
  val defaultRDD: RDD[Item] = ssc.sparkContext.parallelize(items)
  val dStream: DStream[Item] = new ConstantInputDStream[Item](ssc, defaultRDD)

  dStream.sendToKafka[ItemJsonSerializer](topic, producerConf)
  dStream.count().print()

  ssc.start()
  ssc.awaitTermination()

  ssc.stop(stopSparkContext = true, stopGracefully = true)
}