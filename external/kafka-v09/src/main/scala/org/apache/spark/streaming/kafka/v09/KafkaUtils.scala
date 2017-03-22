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

package org.apache.spark.streaming.kafka.v09

import java.{util => ju}
import java.io.OutputStream
import java.lang.{Integer => JInt, Long => JLong}

import scala.collection.JavaConverters._
import com.google.common.base.Charsets.UTF_8
import kafka.common.TopicAndPartition
import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SslConfigs
import org.apache.spark.{Logging, SSLOptions, SparkContext}
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaInputDStream, JavaPairInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.reflect.ClassTag

/**
 * :: Experimental ::
 * object for constructing Kafka streams and RDDs
 */
@Experimental
object KafkaUtils extends Logging {
  /**
   * :: Experimental ::
   * Scala constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K, V, R](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy,
      messageHandler: ConsumerRecord[K, V] => R
    )(implicit kTag: ClassTag[K],
      vTag: ClassTag[V],
      rTag: ClassTag[R]
    ): RDD[R] = {

    val preferredHosts = locationStrategy match {
      case PreferBrokers =>
        throw new AssertionError(
          "If you want to prefer brokers, you must provide a mapping using PreferFixed " +
          "A single KafkaRDD does not have a driver consumer and cannot look up brokers for you.")
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) => hostMap
    }
    val kp = new ju.HashMap[String, Object](kafkaParams)
    fixKafkaParams(kp)
    val osr = offsetRanges.clone()

    new KafkaRDD[K, V, R](
      sc,
      kp,
      osr,
      preferredHosts,
      true,
      messageHandler
      )
  }

  /**
  * :: Experimental ::
  * Java constructor for a batch-oriented interface for consuming from Kafka.
  * Starting and ending offsets are specified in advance,
  * so that you can control exactly-once semantics.
  *
  * @param kafkaParams      Kafka
  *                     <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
  *                         configuration parameters</a>. Requires "bootstrap.servers" to be set
  *                         with Kafka broker(s) specified in host1:port1,host2:port2 form.
  * @param offsetRanges     offset ranges that define the Kafka data belonging to this RDD
  * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
  *                         see [[LocationStrategies]] for more details.
  * @tparam K type of Kafka message key
  * @tparam V type of Kafka message value
  */
  @Experimental
  def createRDD[K, V](
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    )(implicit recordClass: ClassTag[K], valueClass: ClassTag[V]
    ): JavaRDD[(K, V)] = {

    val messageHandler = (cr: ConsumerRecord[K, V]) => (cr.key(), cr.value())
    new JavaRDD(createRDD[K, V, (K, V)](
      jsc.sc,
      kafkaParams,
      offsetRanges,
      locationStrategy,
      messageHandler
    ))
  }

  /**
  * :: Experimental ::
  * Scala constructor for a DStream where
  * each given Kafka topic/partition corresponds to an RDD partition.
  * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
  * of messages
  * per second that each '''partition''' will accept.
  *
  * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
  *                         see [[LocationStrategies]] for more details.
  * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
  *                         see [[ConsumerStrategies]] for more details
  * @tparam K type of Kafka message key
  * @tparam V type of Kafka message value
  */
  @Experimental
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    )(implicit recordClass: ClassTag[K], valueClass: ClassTag[V]
    ): InputDStream[(K, V)] = {

    val messageHandler = (cr: ConsumerRecord[K, V]) => (cr.key(), cr.value())
    new DirectKafkaInputDStream[K, V, (K, V)](
      ssc,
      locationStrategy,
      consumerStrategy,
      messageHandler
    )
  }

  /**
  * :: Experimental ::
  * Java constructor for a DStream where
  * each given Kafka topic/partition corresponds to an RDD partition.
  *
  * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
  *                         see [[LocationStrategies]] for more details.
  * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
  *                         see [[ConsumerStrategies]] for more details
  * @tparam K type of Kafka message key
  * @tparam V type of Kafka message value
  */
  @Experimental
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    )(implicit recordClass: ClassTag[K], valueClass: ClassTag[V]): JavaInputDStream[(K, V)] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc,
        locationStrategy,
        consumerStrategy
      ))
  }

  /**
  * Tweak kafka params to prevent issues on executors
  */
  private[v09] def fixKafkaParams(kafkaParams: ju.HashMap[String, Object]): Unit = {

    logWarning(s"overriding ${ConsumerConfig.STREAMS_ZEROOFFSET_RECORD_ON_EOF_CONFIG} to true")
    kafkaParams.put(ConsumerConfig.STREAMS_ZEROOFFSET_RECORD_ON_EOF_CONFIG, true: java.lang.Boolean)

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

  // *************************** OLD API ******************************** //
  def addSSLOptions(
      kafkaParams: Map[String, String],
      sc: SparkContext
    ): Map[String, String] = {

    val sparkConf = sc.getConf
    val defaultSSLOptions = SSLOptions.parse(sparkConf, "spark.ssl", None)
    val kafkaSSLOptions = SSLOptions.parse(sparkConf, "spark.ssl.kafka", Some(defaultSSLOptions))

    if (kafkaSSLOptions.enabled) {
      val sslParams = Map[String, Option[_]](
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> Some("SSL"),
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> kafkaSSLOptions.trustStore,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> kafkaSSLOptions.trustStorePassword,
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> kafkaSSLOptions.keyStore,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> kafkaSSLOptions.keyStorePassword,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG -> kafkaSSLOptions.keyPassword)
      kafkaParams ++ sslParams.filter(_._2.isDefined).mapValues(_.get.toString)
    } else {
      kafkaParams
    }
  }

  /**
    * Create a RDD from Kafka using offset ranges for each topic and partition.
    *
    * @param sc SparkContext object
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                    configuration parameters</a>. Requires "bootstrap.servers"
    *                    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
    *                    host1:port1,host2:port2 form.
    * @param offsetRanges Each OffsetRange in the batch corresponds to a
    *                     range of offsets for a given Kafka topic/partition
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @return RDD of (Kafka message key, Kafka message value)
    */
  def createRDD[K: ClassTag, V: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange]
    ): RDD[(K, V)] = {

    createRDD[K, V](
      sc,
      new ju.HashMap[String, Object](kafkaParams.asJava),
      offsetRanges,
      LocationStrategies.PreferConsistent
    )
  }

  /**
    * Create a RDD from Kafka using offset ranges for each topic and partition. This allows you
    * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
    * as the metadata.
    *
    * @param sc SparkContext object
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                    configuration parameters</a>. Requires "bootstrap.servers"
    *                    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
    *                    host1:port1,host2:port2 form.
    * @param offsetRanges Each OffsetRange in the batch corresponds to a
    *                     range of offsets for a given Kafka topic/partition
    * @param messageHandler Function for translating each message and metadata into the desired type
    *                       * @tparam K type of Kafka message key
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @tparam R type returned by messageHandler
    * @return RDD of R
    */
  def createRDD[K: ClassTag, V: ClassTag, R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange],
      messageHandler: ConsumerRecord[K, V] => R
    ): RDD[R] = {

    val cleanedHandler = sc.clean(messageHandler)
    createRDD[K, V, R](
      sc,
      new ju.HashMap[String, Object](kafkaParams.asJava),
      offsetRanges,
      LocationStrategies.PreferConsistent,
      cleanedHandler
    )

  }

  /**
    * Create a RDD from Kafka using offset ranges for each topic and partition.
    *
    * @param jsc JavaSparkContext object
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                    configuration parameters</a>. Requires "bootstrap.servers"
    *                    specified in host1:port1,host2:port2 form.
    * @param offsetRanges Each OffsetRange in the batch corresponds to a
    *                     range of offsets for a given Kafka topic/partition
    * @param keyClass type of Kafka message key
    * @param valueClass type of Kafka message value
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @return RDD of (Kafka message key, Kafka message value)
    */
  def createRDD[K, V]( // TODO pay attention on testing
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      kafkaParams: ju.Map[String, String],
      offsetRanges: Array[OffsetRange]
    ): JavaPairRDD[K, V] = {

    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)

    new JavaPairRDD(createRDD[K, V](
      jsc.sc, Map(kafkaParams.asScala.toSeq: _*), offsetRanges))
  }

  /**
    * Create a RDD from Kafka using offset ranges for each topic and partition. This allows you
    * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
    * as the metadata.
    *
    * @param jsc JavaSparkContext object
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                    configuration parameters</a>. Requires "bootstrap.servers"
    *                    specified in host1:port1,host2:port2 form.
    * @param offsetRanges Each OffsetRange in the batch corresponds to a
    *                     range of offsets for a given Kafka topic/partition
    * @param messageHandler Function for translating each message and metadata into the desired type
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @tparam R type returned by messageHandler
    * @return RDD of R
    */
  def createRDD[K, V, R](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      recordClass: Class[R],
      kafkaParams: ju.Map[String, String],
      offsetRanges: Array[OffsetRange],
      messageHandler: JFunction[ConsumerRecord[K, V], R]
    ): JavaRDD[R] = {

    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)

    new JavaRDD[R](createRDD[K, V, R](
      jsc.sc,
      new ju.HashMap[String, Object](kafkaParams),
      offsetRanges,
      LocationStrategies.PreferConsistent,
      messageHandler.call _
    ))
  }

  /**
    * Create an input stream that directly pulls messages from Kafka Brokers
    * without using any receiver. This stream can guarantee that each message
    * from Kafka is included in transformations exactly once (see points below).
    *
    * Points to note:
    * - No receivers: This stream does not use any receiver. It directly queries Kafka
    * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
    * by the stream itself.
    * You can access the offsets used in each batch from the generated RDDs (see
    * [[org.apache.spark.streaming.kafka.v09.HasOffsetRanges]]).
    * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
    * in the [[StreamingContext]]. The information on consumed offset can be
    * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
    * - End-to-end semantics: This stream ensures that every records is effectively received and
    * transformed exactly once, but gives no guarantees on whether the transformed data are
    * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
    * that the output operation is idempotent, or use transactions to output records atomically.
    * See the programming guide for more details.
    *
    * @param ssc StreamingContext object
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                    configuration parameters</a>. Requires "bootstrap.servers"
    *                    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
    *                    host1:port1,host2:port2 form.
    * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
    *                    starting point of the stream
    * @param messageHandler Function for translating each message and metadata into the desired type
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @tparam R type returned by messageHandler
    * @return DStream of R
    */
  def createDirectStream[K: ClassTag, V: ClassTag, R: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicPartition, Long],
      messageHandler: ConsumerRecord[K, V] => R
    ): InputDStream[R] = {

    val consumerStrategy = ConsumerStrategies.Assign[K, V](
      fromOffsets.keys,
      kafkaParams,
      fromOffsets
    )

    val cleanedHandler = ssc.sc.clean(messageHandler)
    new DirectKafkaInputDStream[K, V, R](
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy,
      cleanedHandler
    )
  }

  /**
    * Create an input stream that directly pulls messages from Kafka Brokers
    * without using any receiver. This stream can guarantee that each message
    * from Kafka is included in transformations exactly once (see points below).
    *
    * Points to note:
    * - No receivers: This stream does not use any receiver. It directly queries Kafka
    * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
    * by the stream itself.
    * You can access the offsets used in each batch from the generated RDDs (see
    * [[org.apache.spark.streaming.kafka.v09.HasOffsetRanges]]).
    * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
    * in the [[StreamingContext]]. The information on consumed offset can be
    * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
    * - End-to-end semantics: This stream ensures that every records is effectively received and
    * transformed exactly once, but gives no guarantees on whether the transformed data are
    * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
    * that the output operation is idempotent, or use transactions to output records atomically.
    * See the programming guide for more details.
    *
    * @param ssc StreamingContext object
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                    configuration parameters</a>. Requires "bootstrap.servers"
    *                    to be set with Kafka broker(s) (NOT zookeeper servers), specified in
    *                    host1:port1,host2:port2 form.
    *                    If not starting from a checkpoint, "auto.offset.reset" may be set to
    *                    "earliest" or "latest" to determine where the stream starts
    *                    (defaults to "latest")
    * @param topics Names of the topics to consume
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @return DStream of (Kafka message key, Kafka message value)
    */
  def createDirectStream[K: ClassTag, V: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
    ): InputDStream[(K, V)] = {

    val consumerStrategy = ConsumerStrategies.Subscribe[K, V](
      topics,
      kafkaParams
    )

    new DirectKafkaInputDStream[K, V, (K, V)](
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy,
      cr => (cr.key, cr.value)
    )
  }

  /**
    * Create an input stream that directly pulls messages from Kafka Brokers
    * without using any receiver. This stream can guarantee that each message
    * from Kafka is included in transformations exactly once (see points below).
    *
    * Points to note:
    * - No receivers: This stream does not use any receiver. It directly queries Kafka
    * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
    * by the stream itself.
    * You can access the offsets used in each batch from the generated RDDs (see
    * [[org.apache.spark.streaming.kafka.v09.HasOffsetRanges]]).
    * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
    * in the [[StreamingContext]]. The information on consumed offset can be
    * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
    * - End-to-end semantics: This stream ensures that every records is effectively received and
    * transformed exactly once, but gives no guarantees on whether the transformed data are
    * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
    * that the output operation is idempotent, or use transactions to output records atomically.
    * See the programming guide for more details.
    *
    * @param jssc JavaStreamingContext object
    * @param keyClass Class of the keys in the Kafka records
    * @param valueClass Class of the values in the Kafka records
    * @param recordClass Class of the records in DStream
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                    configuration parameters</a>. Requires "bootstrap.servers"
    *                    specified in host1:port1,host2:port2 form.
    * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
    *                    starting point of the stream
    * @param messageHandler Function for translating each message and metadata into the desired type
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @tparam R type returned by messageHandler
    * @return DStream of R
    */
  def createDirectStream[K, V, R](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      recordClass: Class[R],
      kafkaParams: ju.Map[String, String],
      fromOffsets: ju.Map[TopicPartition, JLong],
      messageHandler: JFunction[ConsumerRecord[K, V], R]
    )(implicit resClass: ClassTag[R]): JavaInputDStream[R] = {

    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)

    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call _)
    createDirectStream[K, V, R](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Map(fromOffsets.asScala.mapValues {
        _.longValue()
      }.toSeq: _*),
      cleanedHandler)
  }

  /**
    * Create an input stream that directly pulls messages from Kafka Brokers
    * without using any receiver. This stream can guarantee that each message
    * from Kafka is included in transformations exactly once (see points below).
    *
    * Points to note:
    * - No receivers: This stream does not use any receiver. It directly queries Kafka
    * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
    * by the stream itself.
    * You can access the offsets used in each batch from the generated RDDs (see
    * [[org.apache.spark.streaming.kafka.v09.HasOffsetRanges]]).
    * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
    * in the [[StreamingContext]]. The information on consumed offset can be
    * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
    * - End-to-end semantics: This stream ensures that every records is effectively received and
    * transformed exactly once, but gives no guarantees on whether the transformed data are
    * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
    * that the output operation is idempotent, or use transactions to output records atomically.
    * See the programming guide for more details.
    *
    * @param jssc JavaStreamingContext object
    * @param keyClass Class of the keys in the Kafka records
    * @param valueClass Class of the values in the Kafka records
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                    configuration parameters</a>. Requires "bootstrap.servers"
    *                    to be set with Kafka broker(s) (NOT zookeeper servers), specified in
    *                    host1:port1,host2:port2 form.
    *                    If not starting from a checkpoint, "auto.offset.reset" may be set
    *                    to "latest" or "earliest" to determine where the stream starts
    *                    (defaults to "latest")
    * @param topics Names of the topics to consume
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @return DStream of (Kafka message key, Kafka message value)
    */
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      kafkaParams: ju.Map[String, String],
      topics: ju.Set[String]
    ): JavaPairInputDStream[K, V] = {

    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)

    createDirectStream[K, V](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Set(topics.asScala.toSeq: _*))
  }

  def createOffsetRange(
      topic: String,
      partition: JInt,
      fromOffset: JLong,
      untilOffset: JLong
    ): OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)

  def createTopicAndPartition(topic: String, partition: JInt): TopicAndPartition =
    TopicAndPartition(topic, partition)
}

/**
  * This is a helper class that wraps the KafkaUtils.createStream() into more
  * Python-friendly class and function so that it can be easily
  * instantiated and called from Python's KafkaUtils (see SPARK-6027).
  *
  * The zero-arg constructor helps instantiate this class from the Class object
  * classOf[KafkaUtilsPythonHelper].newInstance(), and the createStream()
  * takes care of known parameters instead of passing them from Python
  */
private[kafka] class KafkaUtilsPythonHelper {

  import KafkaUtilsPythonHelper._

  def createRDDWithoutMessageHandler(
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, String],
      offsetRanges: ju.List[OffsetRange]
    ): JavaRDD[(Array[Byte], Array[Byte])] = {

    val messageHandler =
      (cr: ConsumerRecord[Array[Byte], Array[Byte]]) => (cr.key, cr.value)
    new JavaRDD(createRDD(jsc, kafkaParams, offsetRanges, messageHandler))
  }

  def createRDDWithMessageHandler(
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, String],
      offsetRanges: ju.List[OffsetRange]
    ): JavaRDD[Array[Byte]] = {

    val messageHandler = (cr: ConsumerRecord[Array[Byte], Array[Byte]]) =>
      new PythonConsumerRecord(
        cr.topic, cr.partition, cr.offset, cr.key(), cr.value())
    val rdd = createRDD(jsc, kafkaParams, offsetRanges, messageHandler).
      mapPartitions(picklerIterator)
    new JavaRDD(rdd)
  }

  private def createRDD[V: ClassTag](
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, String],
      offsetRanges: ju.List[OffsetRange],
      messageHandler: ConsumerRecord[Array[Byte], Array[Byte]] => V
    ): RDD[V] = {

    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    KafkaUtils.createRDD[Array[Byte], Array[Byte], V](
      jsc.sc,
      kafkaParams.asScala.toMap,
      offsetRanges.toArray(new Array[OffsetRange](offsetRanges.size())),
      messageHandler)
  }

  def createDirectStreamWithoutMessageHandler(
      jssc: JavaStreamingContext,
      kafkaParams: ju.Map[String, String],
      topics: ju.Set[String],
      fromOffsets: ju.Map[TopicPartition, JLong]
    ): JavaDStream[(Array[Byte], Array[Byte])] = {

    val messageHandler =
      (cr: ConsumerRecord[Array[Byte], Array[Byte]]) => (cr.key, cr.value)

    new JavaDStream(
      createDirectStream(jssc, kafkaParams, topics, fromOffsets, messageHandler))
  }

  def createDirectStreamWithMessageHandler(
      jssc: JavaStreamingContext,
      kafkaParams: ju.Map[String, String],
      topics: ju.Set[String],
      fromOffsets: ju.Map[TopicPartition, JLong]
    ): JavaDStream[Array[Byte]] = {

    val messageHandler = (cr: ConsumerRecord[Array[Byte], Array[Byte]]) =>
      new PythonConsumerRecord(cr.topic, cr.partition, cr.offset, cr.key(), cr.value())
    val stream = createDirectStream(jssc, kafkaParams, topics, fromOffsets, messageHandler).
      mapPartitions(picklerIterator)

    new JavaDStream(stream)
  }

  private def createDirectStream[V: ClassTag](
      jssc: JavaStreamingContext,
      kafkaParams: ju.Map[String, String],
      topics: ju.Set[String],
      fromOffsets: ju.Map[TopicPartition, JLong],
      messageHandler: ConsumerRecord[Array[Byte], Array[Byte]] => V
    ): DStream[V] = {

    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    val consumerStrategy = ConsumerStrategies.Subscribe[Array[Byte], Array[Byte]](
      topics.asScala,
      kafkaParams.asScala
    )

    new DirectKafkaInputDStream[Array[Byte], Array[Byte], V](
      jssc.ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy,
      messageHandler
    )
  }

  def createOffsetRange(
      topic: String,
      partition: JInt,
      fromOffset: JLong,
      untilOffset: JLong
    ): OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)

  def createTopicAndPartition(topic: String, partition: JInt): TopicPartition =
    new TopicPartition(topic, partition)

  def offsetRangesOfKafkaRDD(rdd: RDD[_]): ju.List[OffsetRange] = {
    val parentRDDs = rdd.getNarrowAncestors
    val kafkaRDDs = parentRDDs.filter(rdd => rdd.isInstanceOf[KafkaRDD[_, _, _]])

    require(
      kafkaRDDs.length == 1,
      "Cannot get offset ranges, as there may be multiple Kafka RDDs or no Kafka RDD associated" +
        "with this RDD, please call this method only on a Kafka RDD.")

    val kafkaRDD = kafkaRDDs.head.asInstanceOf[KafkaRDD[_, _, _]]
    kafkaRDD.offsetRanges.toSeq.asJava
  }
}

object KafkaUtilsPythonHelper {
  private var initialized = false

  def initialize(): Unit = {
    SerDeUtil.initialize()
    synchronized {
      if (!initialized) {
        new PythonConsumerRecordPickler().register()
        initialized = true
      }
    }
  }

  initialize()

  def picklerIterator(iter: Iterator[Any]): Iterator[Array[Byte]] = {
    new SerDeUtil.AutoBatchedPickler(iter)
  }

  case class PythonConsumerRecord(
    topic: String,
    partition: JInt,
    offset: JLong,
    key: Array[Byte],
    message: Array[Byte])

  class PythonConsumerRecordPickler extends IObjectPickler {
    private val module = "pyspark.streaming.kafka"

    def register(): Unit = {
      Pickler.registerCustomPickler(classOf[PythonConsumerRecord], this)
      Pickler.registerCustomPickler(this.getClass, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler) {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write(s"$module\nKafkaMessageAndMetadata\n".getBytes(UTF_8))
      } else {
        pickler.save(this)
        val msgAndMetaData = obj.asInstanceOf[PythonConsumerRecord]
        out.write(Opcodes.MARK)
        pickler.save(msgAndMetaData.topic)
        pickler.save(msgAndMetaData.partition)
        pickler.save(msgAndMetaData.offset)
        pickler.save(msgAndMetaData.key)
        pickler.save(msgAndMetaData.message)
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
    }
  }

}
