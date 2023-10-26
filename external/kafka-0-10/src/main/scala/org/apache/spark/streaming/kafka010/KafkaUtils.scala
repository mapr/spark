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

package org.apache.spark.streaming.kafka010

import java.io.OutputStream

import scala.collection.JavaConverters._
import com.google.common.base.Charsets.UTF_8
import java.{util => ju}

import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

import scala.util.{Failure, Success, Try}

/**
 * object for constructing Kafka streams and RDDs
 */
object KafkaUtils extends Logging {
  /**
   * Scala constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createRDD[K, V](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): RDD[ConsumerRecord[K, V]] = {
    val preferredHosts = locationStrategy match {
      case PreferBrokers =>
        throw new IllegalArgumentException(
          "If you want to prefer brokers, you must provide a mapping using PreferFixed " +
          "A single KafkaRDD does not have a driver consumer and cannot look up brokers for you.")
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) => hostMap
    }
    val kp = new ju.HashMap[String, Object](kafkaParams)
    fixKafkaParams(kp)
    val osr = offsetRanges.clone()

    new KafkaRDD[K, V](sc, kp, osr, preferredHosts, true)
  }

  /**
   * Java constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createRDD[K, V](
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): JavaRDD[ConsumerRecord[K, V]] = {

    new JavaRDD(createRDD[K, V](jsc.sc, kafkaParams, offsetRanges, locationStrategy))
  }

  /**
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
   *  of messages
   * per second that each '''partition''' will accept.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): InputDStream[ConsumerRecord[K, V]] = {
    val ppc = new DefaultPerPartitionConfig(ssc.sparkContext.getConf)
    createDirectStream[K, V](ssc, locationStrategy, consumerStrategy, ppc)
  }

  /**
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details.
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V],
      perPartitionConfig: PerPartitionConfig
    ): InputDStream[ConsumerRecord[K, V]] = {
    new DirectKafkaInputDStream[K, V](ssc, locationStrategy, consumerStrategy, perPartitionConfig)
  }

  /**
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, locationStrategy, consumerStrategy))
  }

  /**
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V],
      perPartitionConfig: PerPartitionConfig
    ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, locationStrategy, consumerStrategy, perPartitionConfig))
  }

  val eofOffset: Int = -1001

  /**
   * Tweak kafka params to prevent issues on executors
   */
  private[kafka010] def fixKafkaParams(kafkaParams: ju.HashMap[String, Object]): Unit = {
    logWarning(s"overriding ${"streams.negativeoffset.record.on.eof"} to true")
    kafkaParams.put("streams.negativeoffset.record.on.eof", true: java.lang.Boolean)

    logWarning(s"overriding ${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG} to false for executor")
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)

    logWarning(s"overriding ${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} to none for executor")
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

    // driver and executor should be in different consumer groups
    val originalGroupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)
    if (null == originalGroupId) {
      logError(s"${ConsumerConfig.GROUP_ID_CONFIG} is null, you should probably set it")
    }
    val groupId = "spark-executor-" + originalGroupId
    logWarning(s"overriding executor ${ConsumerConfig.GROUP_ID_CONFIG} to ${groupId}")
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    // possible workaround for KAFKA-3135
    val rbb = kafkaParams.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG)
    if (null == rbb || rbb.asInstanceOf[java.lang.Integer] < 65536) {
      logWarning(s"overriding ${ConsumerConfig.RECEIVE_BUFFER_CONFIG} to 65536 see KAFKA-3135")
      kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
    }
  }

  def waitForConsumerAssignment[K, V](consumer: Consumer[K, V],
                                      partitions: ju.Set[TopicPartition] = new ju.HashSet()): ju.Set[TopicPartition] = {
    val waitingForAssigmentTimeout: Long =
      Try(SparkEnv.get.conf.getLong("spark.mapr.WaitingForAssignmentTimeout", 600000)) match {
        case Success(value) => value
        case Failure(_) => return consumer.assignment()
      }

    var timeout = 0
    var newPartitions = consumer.assignment()
    while ((newPartitions.isEmpty || newPartitions.size() < partitions.size)
      && timeout < waitingForAssigmentTimeout) {
      logWarning(s"Assignment() returned fewer partitions than the previous call: " +
          s"${newPartitions.asScala.mkString("[", ",", "]")}")

      Thread.sleep(500)
      timeout += 500

      newPartitions = consumer.assignment()
    }

    if (timeout >= waitingForAssigmentTimeout) {
      logError(
        s"""Consumer assignment wasn't completed within the timeout $waitingForAssigmentTimeout.
           |Assigned partitions: ${consumer.assignment()}.""".stripMargin)
    }

    newPartitions
  }

  // Determine if Apache Kafka is used instead of MapR Streams
  def isStreams(currentOffsets: Map[TopicPartition, Long]): Boolean =
    currentOffsets.keys.map(_.topic()).exists(topic => topic.startsWith("/") && topic.contains(":"))
}

object KafkaUtilsPythonHelper {
  private var initialized = false

  def initialize(): Unit = {
    SerDeUtil.initialize()
    synchronized {
      if (!initialized) {
        new PythonMessageAndMetadataPickler().register()
        initialized = true
      }
    }
  }

  initialize()

  def picklerIterator(iter: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]]
    ): Iterator[Array[Byte]] = {
    new SerDeUtil.AutoBatchedPickler(iter)
  }

  class PythonMessageAndMetadataPickler extends IObjectPickler {
    private val module = "pyspark.streaming.kafka"

    def register(): Unit = {
      Pickler.registerCustomPickler(classOf[ConsumerRecord[Any, Any]], this)
      Pickler.registerCustomPickler(this.getClass, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler) {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write(s"$module\nKafkaMessageAndMetadata\n".getBytes(UTF_8))
      } else {
        pickler.save(this)
        val msgAndMetaData = obj.asInstanceOf[ConsumerRecord[Array[Byte], Array[Byte]]]
        out.write(Opcodes.MARK)
        pickler.save(msgAndMetaData.topic)
        pickler.save(msgAndMetaData.partition)
        pickler.save(msgAndMetaData.offset)
        pickler.save(msgAndMetaData.key)
        pickler.save(msgAndMetaData.value)
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
    }
  }

  @Experimental
  def createDirectStream(
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[Array[Byte], Array[Byte]]
    ): JavaDStream[(Array[Byte], Array[Byte])] = {

    val dStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      jssc.ssc, locationStrategy, consumerStrategy)
      .map(cm => (cm.key, cm.value))

    new JavaDStream[(Array[Byte], Array[Byte])](dStream)
  }

  @Experimental
  def createRDDWithoutMessageHandler(jsc: JavaSparkContext,
                                     kafkaParams: ju.Map[String, Object],
                                     offsetRanges: ju.List[OffsetRange],
                                     locationStrategy: LocationStrategy): JavaRDD[(Array[Byte], Array[Byte])] = {

    val rdd = KafkaUtils.createRDD[Array[Byte], Array[Byte]](
      jsc.sc, kafkaParams, offsetRanges.asScala.toArray, locationStrategy)
      .map(cm => (cm.key, cm.value))

    new JavaRDD[(Array[Byte], Array[Byte])](rdd)
  }

  @Experimental
  def createOffsetRange(topic: String, partition: Integer, fromOffset: java.lang.Long, untilOffset: java.lang.Long
                       ): OffsetRange = OffsetRange(topic, partition, fromOffset, untilOffset)

  @Experimental
  def createTopicAndPartition(topic: String, partition: java.lang.Integer): TopicPartition =
    new TopicPartition(topic, partition)

  @Experimental
  def offsetRangesOfKafkaRDD(rdd: RDD[_]): ju.List[OffsetRange] = {
    val parentRDDs = rdd.getNarrowAncestors
    val kafkaRDDs = parentRDDs.filter(rdd => rdd.isInstanceOf[KafkaRDD[_, _]])

    require(
      kafkaRDDs.length == 1,
      "Cannot get offset ranges, as there may be multiple Kafka RDDs or no Kafka RDD associated" +
        "with this RDD, please call this method only on a Kafka RDD.")

    val kafkaRDD = kafkaRDDs.head.asInstanceOf[KafkaRDD[_, _]]
    kafkaRDD.offsetRanges.toSeq.asJava
  }
}
