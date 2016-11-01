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

"""
 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/direct_kafka_wordcount.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka09 import KafkaUtils
from pyspark.streaming.kafka09 import ConsumerStrategies
from pyspark.streaming.kafka09 import LocationStrategies

if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic> <group_id> " +
              "<offset_reset> <batch_interval> <poll_timeout>", file=sys.stderr)
        exit(-1)

    brokers, topic, group_id, offset_reset, batch_interval, poll_timeout = sys.argv[1:]

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, int(batch_interval))

    kafkaParams = {
        "bootstrap.servers": brokers,
        "group.id": group_id,
        "key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": "false",
        "spark.kafka.poll.time": poll_timeout
    }

    consumerStrategy = ConsumerStrategies.Subscribe(sc, [topic], kafkaParams)
    locationStrategy = LocationStrategies.PreferConsistent(sc)
    kvs = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
