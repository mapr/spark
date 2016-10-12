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

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

class StreamProducerSuite extends BaseKafkaProducerTest {

  private val numMessagesPerBatch = 10
  private val numBatches = 10
  private val recordKey = "foo"
  private val recordValue = "bar"
  private val batchTime = Seconds(1)
  private val partition = 0
  private val testConf = new ProducerConf(bootstrapServers = List("localhost:9092"))
  private var streamingContext: StreamingContext = _

  before {
    streamingContext = setupSparkStreamingContext()
  }

  after {
    Option(streamingContext).foreach(_.stop(stopSparkContext = true, stopGracefully = true))
    streamingContext = null.asInstanceOf[StreamingContext]
  }

  test("producer should send stream to kafka") {
    val topic = createTestTopic("value.stream.kafka")
    val consumer = consumerForTopic(topic)

    val messages = List.fill(numMessagesPerBatch)(recordValue)
    val rdd = streamingContext.sparkContext.parallelize(messages, 10)

    val rdds = List.fill(numBatches)(rdd)

    val stream = streamingContext.queueStream(mutable.Queue[RDD[String]](rdds: _*))

    stream.sendToKafka[StringSerializer](topic, testConf)

    streamingContext.start()

    val buffer = mutable.ArrayBuffer.empty[ConsumerRecord[String, String]]

    consumer.assign(List(new TopicPartition(topic, partition)).asJava)
    eventually(timeout(20 seconds), interval(200 milliseconds)) {
      val observed = consumer.poll(1000)
      buffer ++= observed.iterator().asScala
      buffer should have length (numBatches * numMessagesPerBatch)
    }

    forAll(buffer) { record =>
      record.topic() shouldEqual topic
      record.partition() shouldEqual partition
      record.key() shouldEqual null.asInstanceOf[Array[Byte]]
      record.value() shouldEqual recordValue
    }

    stream.stop()
  }

  test("producer should send pair stream to kafka") {
    val topic = createTestTopic("key.value.stream.kafka")
    val consumer = consumerForTopic(topic)

    val messages = List.fill(numMessagesPerBatch)(recordKey -> recordValue)
    val rdd = streamingContext.sparkContext.parallelize(messages, 10)

    val rdds = List.fill(numBatches)(rdd)

    val stream = streamingContext.queueStream(mutable.Queue[RDD[(String, String)]](rdds: _*))

    stream.sendToKafka[StringSerializer, StringSerializer](topic, testConf)

    streamingContext.start()

    val buffer = mutable.ArrayBuffer.empty[ConsumerRecord[String, String]]

    consumer.assign(List(new TopicPartition(topic, partition)).asJava)
    eventually(timeout(20 seconds), interval(200 milliseconds)) {
      val observed = consumer.poll(1000)
      buffer ++= observed.iterator().asScala
      buffer should have length (numBatches * numMessagesPerBatch)
    }

    forAll(buffer) { record =>
     record.topic() shouldEqual topic
     record.partition() shouldEqual partition
     record.key() shouldEqual recordKey
     record.value() shouldEqual recordValue
    }

    stream.stop()
  }

  def setupSparkStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
    conf
      .setAppName(classOf[StreamProducerSuite].getCanonicalName)
      .setMaster("local[*]")
    new StreamingContext(conf, batchTime)
  }
}
