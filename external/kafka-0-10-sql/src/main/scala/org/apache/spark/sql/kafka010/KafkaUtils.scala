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

package org.apache.spark.sql.kafka010

import java.{util => ju}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition

object KafkaUtils extends Logging {

  def waitForConsumerAssignment[K, V](consumer: Consumer[Array[Byte], Array[Byte]],
                                      partitions:
                                      ju.Set[TopicPartition]): Unit = {
    val waitingForAssigmentTimeout = SparkEnv.get.conf.
      getLong("spark.mapr.WaitingForAssignmentTimeout", 600000)

    var timeout = 0
    while ((consumer.assignment().isEmpty || consumer.assignment().size() < partitions.size)
      && timeout < waitingForAssigmentTimeout) {

      Thread.sleep(500)
      timeout += 500
    }
  }

  // Determine if Apache Kafka is used instead of MapR Streams
  def isStreams(currentOffsets: Map[TopicPartition, Long]): Boolean =
    currentOffsets.keys.map(_.topic()).exists(topic => topic.startsWith("/") && topic.contains(":"))
}
