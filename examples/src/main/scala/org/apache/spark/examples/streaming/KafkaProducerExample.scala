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

package org.apache.spark.examples.streaming

import java.util.{Map => JMap}

import org.apache.kafka.common.serialization.Serializer
import org.apache.spark.SparkConf

import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import  org.apache.spark.streaming.kafka.v2.producer._
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, LongType}

object KakfaProducerExample extends App {

  if (args.length < 2) {
    System.err.println(s"""
                          |Usage: Usage: KafkaProducerExample  <topics> <numMessages>
                          |  <topics> is a list of one or more kafka topics
                          |  <numMessages> is the number of messages that the kafka producer
                          |                should send
      """.stripMargin)
    System.exit(1)
  }

  val Array(topics, numMessages) = args

  val sparkConf = new SparkConf()
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .setAppName(getClass.getCanonicalName)

  implicit val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val items = (0 until numMessages.toInt).map(_.toString)

  val stringRDD: RDD[String] = sparkSession.sparkContext.parallelize(items)

  // if we have RDD[String] we can write to kafka using the new API V2

  stringRDD.sendToKafka(topics)

  val rnd = new Random()

  // create RDD of Rows
  val anotherRDD = stringRDD.map(s => Row(s, s.length, rnd.nextLong()))

  val schema = new StructType()
    .add(StructField("value", StringType))
    .add(StructField("length", IntegerType))
    .add(StructField("some_long", LongType))

  // create a dataframe with some schema
  val dataFrame: DataFrame = sparkSession.createDataFrame(anotherRDD, schema)

  // any data frame can be easily written to Kafka
  dataFrame.sendToKafka(topics)

  val intRDD: RDD[(Int, Int)] = sparkSession.sparkContext.parallelize(0 until numMessages.toInt).map(n => (n, n.toString.length))

  val transformer = (v: (Int, Int)) => Row(v._1, v._2)

  // given an RDD[A], a function A => Row and a schema, we can write to kafka easily
  intRDD.sendToKafka(topics, transformer, new StructType().add(StructField("value", IntegerType)).add(StructField("length", IntegerType)))

  val batchTime = Seconds(2)
  val ssc = new StreamingContext(sparkConf, batchTime)

  val stringStream: DStream[String] = new ConstantInputDStream[String](ssc, stringRDD)

  stringStream.sendToKafka(topics)

  val someStream = new ConstantInputDStream[(Int, Int)](ssc, intRDD)

  someStream.sendToKafka(topics, transformer, new StructType().add(StructField("value", IntegerType)).add(StructField("length", IntegerType)))

  ssc.start()
  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true, stopGracefully = true)
}