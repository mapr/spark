#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.rdd import RDD
from pyspark.serializers import PairDeserializer, NoOpSerializer
from pyspark.streaming import DStream
from pyspark.streaming.dstream import TransformedDStream
from pyspark.streaming.util import TransformFunction

__all__ = ['Broker', 'KafkaMessageAndMetadata', 'KafkaUtils', 'OffsetRange',
           'TopicAndPartition', 'utf8_decoder', 'LocationStrategies', 'ConsumerStrategies']


def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('utf-8')


class KafkaUtils(object):

    @staticmethod
    def createDirectStream(ssc, locationStrategy, consumerStrategy):
        """
        .. note:: Experimental

        Create an input stream that directly pulls messages from a Kafka Broker and specific offset.

        This is not a receiver based Kafka input stream, it directly pulls the message from Kafka
        in each batch duration and processed without storing.

        This does not use Zookeeper to store offsets. The consumed offsets are tracked
        by the stream itself. For interoperability with Kafka monitoring tools that depend on
        Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
        You can access the offsets used in each batch from the generated RDDs (see

        To recover from driver failures, you have to enable checkpointing in the StreamingContext.
        The information on consumed offset can be recovered from the checkpoint.
        See the programming guide for details (constraints, etc.).

        :param ssc:  StreamingContext object.
        :param locationStrategy: In most cases, pass in LocationStrategies.preferConsistent,
            see [[LocationStrategies]] for more details.
        :param consumerStrategy: In most cases, pass in ConsumerStrategies.subscribe,
            see [[ConsumerStrategies]] for more details
        :return: A DStream object
        """

        def funcWithoutMessageHandler(k_v):
            return (utf8_decoder(k_v[0]), utf8_decoder(k_v[1]))

        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        jstream = ssc._sc._jvm.org.apache.spark.streaming.kafka09.KafkaUtilsPythonHelper. \
            createDirectStream(ssc._jssc, locationStrategy, consumerStrategy)

        stream = DStream(jstream, ssc, ser).map(funcWithoutMessageHandler)
        return KafkaDStream(stream._jdstream, ssc, stream._jrdd_deserializer)

    @staticmethod
    def _get_helper(sc):
        try:
            return sc._jvm.org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper()
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                KafkaUtils._printErrorMsg(sc)
            raise

    @staticmethod
    def _printErrorMsg(sc):
        print("""
________________________________________________________________________________________________

  Spark Streaming's Kafka libraries not found in class path. Try one of the following.

  1. Include the Kafka library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-kafka-0-9-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-kafka-0-9-assembly.jar> ...

________________________________________________________________________________________________

""" % (sc.version, sc.version))


class OffsetRange(object):
    """
    Represents a range of offsets from a single Kafka TopicAndPartition.
    """

    def __init__(self, topic, partition, fromOffset, untilOffset):
        """
        Create an OffsetRange to represent range of offsets
        :param topic: Kafka topic name.
        :param partition: Kafka partition id.
        :param fromOffset: Inclusive starting offset.
        :param untilOffset: Exclusive ending offset.
        """
        self.topic = topic
        self.partition = partition
        self.fromOffset = fromOffset
        self.untilOffset = untilOffset

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.topic == other.topic
                    and self.partition == other.partition
                    and self.fromOffset == other.fromOffset
                    and self.untilOffset == other.untilOffset)
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "OffsetRange(topic: %s, partition: %d, range: [%d -> %d]" \
               % (self.topic, self.partition, self.fromOffset, self.untilOffset)

    def _jOffsetRange(self, helper):
        return helper.createOffsetRange(self.topic, self.partition, self.fromOffset,
                                        self.untilOffset)


class TopicAndPartition(object):
    """
    Represents a specific top and partition for Kafka.
    """

    def __init__(self, topic, partition):
        """
        Create a Python TopicAndPartition to map to the Java related object
        :param topic: Kafka topic name.
        :param partition: Kafka partition id.
        """
        self._topic = topic
        self._partition = partition

    def _jTopicAndPartition(self, helper):
        return helper.createTopicAndPartition(self._topic, self._partition)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self._topic == other._topic
                    and self._partition == other._partition)
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return (self._topic, self._partition).__hash__()


class Broker(object):
    """
    Represent the host and port info for a Kafka broker.
    """

    def __init__(self, host, port):
        """
        Create a Python Broker to map to the Java related object.
        :param host: Broker's hostname.
        :param port: Broker's port.
        """
        self._host = host
        self._port = port

    def _jBroker(self, helper):
        return helper.createBroker(self._host, self._port)


class KafkaRDD(RDD):
    """
    A Python wrapper of KafkaRDD, to provide additional information on normal RDD.
    """

    def __init__(self, jrdd, ctx, jrdd_deserializer):
        RDD.__init__(self, jrdd, ctx, jrdd_deserializer)

    def offsetRanges(self):
        """
        Get the OffsetRange of specific KafkaRDD.
        :return: A list of OffsetRange
        """
        helper = KafkaUtils._get_helper(self.ctx)
        joffsetRanges = helper.offsetRangesOfKafkaRDD(self._jrdd.rdd())
        ranges = [OffsetRange(o.topic(), o.partition(), o.fromOffset(), o.untilOffset())
                  for o in joffsetRanges]
        return ranges


class KafkaDStream(DStream):
    """
    A Python wrapper of KafkaDStream
    """

    def __init__(self, jdstream, ssc, jrdd_deserializer):
        DStream.__init__(self, jdstream, ssc, jrdd_deserializer)

    def foreachRDD(self, func):
        """
        Apply a function to each RDD in this DStream.
        """
        if func.__code__.co_argcount == 1:
            old_func = func
            func = lambda r, rdd: old_func(rdd)
        jfunc = TransformFunction(self._sc, func, self._jrdd_deserializer) \
            .rdd_wrapper(lambda jrdd, ctx, ser: KafkaRDD(jrdd, ctx, ser))
        api = self._ssc._jvm.PythonDStream
        api.callForeachRDD(self._jdstream, jfunc)

    def transform(self, func):
        """
        Return a new DStream in which each RDD is generated by applying a function
        on each RDD of this DStream.

        `func` can have one argument of `rdd`, or have two arguments of
        (`time`, `rdd`)
        """
        if func.__code__.co_argcount == 1:
            oldfunc = func
            func = lambda t, rdd: oldfunc(rdd)
        assert func.__code__.co_argcount == 2, "func should take one or two arguments"

        return KafkaTransformedDStream(self, func)


class KafkaTransformedDStream(TransformedDStream):
    """
    Kafka specific wrapper of TransformedDStream to transform on Kafka RDD.
    """

    def __init__(self, prev, func):
        TransformedDStream.__init__(self, prev, func)

    @property
    def _jdstream(self):
        if self._jdstream_val is not None:
            return self._jdstream_val

        jfunc = TransformFunction(self._sc, self.func, self.prev._jrdd_deserializer) \
            .rdd_wrapper(lambda jrdd, ctx, ser: KafkaRDD(jrdd, ctx, ser))
        dstream = self._sc._jvm.PythonTransformedDStream(self.prev._jdstream.dstream(), jfunc)
        self._jdstream_val = dstream.asJavaDStream()
        return self._jdstream_val


class KafkaMessageAndMetadata(object):
    """
    Kafka message and metadata information. Including topic, partition, offset and message
    """

    def __init__(self, topic, partition, offset, key, message):
        """
        Python wrapper of Kafka MessageAndMetadata
        :param topic: topic name of this Kafka message
        :param partition: partition id of this Kafka message
        :param offset: Offset of this Kafka message in the specific partition
        :param key: key payload of this Kafka message, can be null if this Kafka message has no key
                    specified, the return data is undecoded bytearry.
        :param message: actual message payload of this Kafka message, the return data is
                        undecoded bytearray.
        """
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self._rawKey = key
        self._rawMessage = message
        self._keyDecoder = utf8_decoder
        self._valueDecoder = utf8_decoder

    def __str__(self):
        return "KafkaMessageAndMetadata(topic: %s, partition: %d, offset: %d, key and message...)" \
               % (self.topic, self.partition, self.offset)

    def __repr__(self):
        return self.__str__()

    def __reduce__(self):
        return (KafkaMessageAndMetadata,
                (self.topic, self.partition, self.offset, self._rawKey, self._rawMessage))

    def _set_key_decoder(self, decoder):
        self._keyDecoder = decoder

    def _set_value_decoder(self, decoder):
        self._valueDecoder = decoder

    @property
    def key(self):
        return self._keyDecoder(self._rawKey)

    @property
    def message(self):
        return self._valueDecoder(self._rawMessage)


class LocationStrategies(object):
    """
    Kafka specific wrapper of LocationStrategy
    """

    @staticmethod
    def PreferBrokers(sc):
        """
        Use this only if your executors are on the same nodes as your Kafka brokers.
        """
        return sc._jvm.org.apache.spark.streaming.kafka09.LocationStrategies.PreferBrokers()

    @staticmethod
    def PreferConsistent(sc):
        """
        .. note:: Experimental
        Use this in most cases, it will consistently distribute partitions across all executors.
        """
        return sc._jvm.org.apache.spark.streaming.kafka09.LocationStrategies.PreferConsistent()

    @staticmethod
    def PreferFixed(sc, host_dict):
        """
        .. note:: Experimental
        Use this to place particular TopicPartitions on particular hosts if your load is uneven.
        Any TopicPartition not specified in the map will use a consistent location.
        :param sc: SparkContext
        :param host_dict: Dictionary of hosts [TopicAndPartition, String]
        :return: LocationStrategy
        """

        jm = sc._jvm.java.util.HashMap()
        if not host_dict:
            host_dict = {}
        for tp, v in host_dict.items():
            jm[tp._jTopicAndPartition] = v

        return sc._jvm.org.apache.spark.streaming.kafka09.LocationStrategies.PreferFixed(jm)


class ConsumerStrategies(object):
    """
    .. note:: Experimental
    Kafka specific wrapper of ConsumerStrategies
    """

    @staticmethod
    def Subscribe(sc, topics, kafkaParams, offsets=None):
        """
        .. note:: Experimental
        Subscribe to a collection of topics.
        :param sc: SparkContext
        :param topics: collection of topics to subscribe
        :param kafkaParams: Kafka
        <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
        configuration parameters</a> to be used on driver. The same params will be used on executors,
        with minor automatic modifications applied.
         Requires "bootstrap.servers" to be set
        with Kafka broker(s) specified in host1:port1,host2:port2 form.
        :param offsets: offsets to begin at on initial startup.  If no offset is given for
        TopicPartition, the committed offset (if applicable) or kafka param
        auto.offset.reset will be used.
        """

        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        if not isinstance(topics, list):
            raise TypeError("topics should be list")

        if offsets is None:
            return sc._jvm.org.apache.spark.streaming.kafka09. \
                ConsumerStrategies.Subscribe(topics, kafkaParams)

        if not isinstance(offsets, dict):
            raise TypeError("offsetRanges should be dict")

        joffsets = sc._jvm.java.util.HashMap()
        for tp, v in offsets.items():
            joffsets[tp._jTopicAndPartition] = v

        return sc._jvm.org.apache.spark.streaming.kafka09. \
            ConsumerStrategies.Subscribe(topics, kafkaParams, joffsets)

    @staticmethod
    def SubscribePattern(sc, pattern, kafkaParams, offsets=None):
        """
        .. note:: Experimental
        Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
        The pattern matching will be done periodically against topics existing at the time of check.
        :param pattern: pattern to subscribe to
        :param kafkaParams: Kafka
        <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
        configuration parameters</a> to be used on driver. The same params will be used on executors,
        with minor automatic modifications applied.
         Requires "bootstrap.servers" to be set
        with Kafka broker(s) specified in host1:port1,host2:port2 form.
        :param offsets: offsets to begin at on initial startup.  If no offset is given for a
           TopicPartition, the committed offset (if applicable) or kafka param
        auto.offset.reset will be used.
        """

        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        jpattern = sc._jvm.java.util.regex.Pattern.compile(pattern)

        if offsets is None:
            return sc._jvm.org.apache.spark.streaming.kafka09. \
                ConsumerStrategies.SubscribePattern(jpattern, kafkaParams)

        if not isinstance(offsets, dict):
            raise TypeError("offsetRanges should be dict")

        joffsets = sc._jvm.java.util.HashMap()
        for tp, v in offsets.items():
            joffsets[tp._jTopicAndPartition] = v

        return sc._jvm.org.apache.spark.streaming.kafka09. \
            ConsumerStrategies.SubscribePattern(jpattern, kafkaParams, joffsets)

    @staticmethod
    def Assign(sc, topicPartitions, kafkaParams, offsets=None):
        """
        .. note:: Experimental
        Assign a fixed collection of TopicPartitions
        :param topicPartitions: collection of TopicPartitions to assign
        :param kafkaParams: Kafka
        <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
        configuration parameters</a> to be used on driver. The same params will be used on executors,
        with minor automatic modifications applied.
         Requires "bootstrap.servers" to be set
        with Kafka broker(s) specified in host1:port1,host2:port2 form.
        :param offsets: offsets to begin at on initial startup.  If no offset is given for a
          * TopicPartition, the committed offset (if applicable) or kafka param
        auto.offset.reset will be used.
        """

        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        if not isinstance(topicPartitions, list):
            raise TypeError("topics should be list")

        jtopicPartitions = sc._jvm.java.util.ArrayList()
        for topicPartition in jtopicPartitions:
            jtopicPartitions.add(topicPartition._jTopicAndPartition)

        if offsets is None:
            return sc._jvm.org.apache.spark.streaming.kafka09. \
                ConsumerStrategies.Assign(jtopicPartitions, kafkaParams)

        if not isinstance(offsets, dict):
            raise TypeError("offsetRanges should be dict")

        joffsets = sc._jvm.java.util.HashMap()
        for tp, v in offsets.items():
            joffsets[tp._jTopicAndPartition] = v

        return sc._jvm.org.apache.spark.streaming.kafka09. \
            ConsumerStrategies.Assign(jtopicPartitions, kafkaParams, joffsets)
