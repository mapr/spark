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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class RDDProducerSuite extends BaseKafkaProducerTest {

  private val numMessages = 10
  private val recordKey = "key"
  private val recordValue = "value"
  private val partition = 0
  private val testConf = new ProducerConf(bootstrapServers = List("localhost:9092"))
  private var sparkContext: SparkContext = _

  before {
    sparkContext = setupSparkContext()
  }

  after {
    Option(sparkContext).foreach(_.stop())
    sparkContext = null.asInstanceOf[SparkContext]
  }

  test("producer should send rdd to kafka") {
    val topic = createTestTopic("value.rdd.kafka")
    val consumer = consumerForTopic(topic)

    val rdd: RDD[String] = sparkContext.parallelize(List.fill(numMessages)(recordValue))

    rdd.sendToKafka[StringSerializer](topic, testConf)

    consumer.assign(List(new TopicPartition(topic, partition)).asJava)

    var observed: Iterator[ConsumerRecord[String, String]] = null

    eventually(timeout(20 seconds), interval(500 milliseconds)) {
      observed = consumer.poll(1000).iterator().asScala
      observed should have size numMessages
    }
    forAll(observed.toList) { record =>
      record.topic() shouldEqual topic
      record.partition() shouldEqual partition
      record.key() shouldEqual null.asInstanceOf[Array[Byte]]
      record.value() shouldEqual recordValue
    }
  }

  test("producer should send pair rdd to kafka") {
    val topic = createTestTopic("key.value.rdd.kafka")
    val consumer = consumerForTopic(topic)

    val rdd = sparkContext.parallelize(List.fill(numMessages)(recordKey, recordValue))

    rdd.sendToKafka[StringSerializer, StringSerializer](topic, testConf)

    consumer.assign(List(new TopicPartition(topic, partition)).asJava)

    var observed: Iterator[ConsumerRecord[String, String]] = null

    eventually(timeout(20 seconds), interval(500 milliseconds)) {
      observed = consumer.poll(1000).iterator().asScala
      observed should have size numMessages
    }
    forAll(observed.toList) { record =>
      record.topic() shouldEqual topic
      record.partition() shouldEqual partition
      record.key() shouldEqual recordKey
      record.value() shouldEqual recordValue
    }
  }

  def setupSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.setAppName(classOf[RDDProducerSuite].getCanonicalName)
      .setMaster("local[*]")
    new SparkContext(conf)
  }
}
