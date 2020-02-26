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

package org.apache.spark.streaming

import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, DirectKafkaInputDStream, HasOffsetRanges}

/**
 * Spark Integration for Kafka 0.9
 */

//scalastyle:off

package object kafka010 {

  /**
    * This extension provides easy access to commit offsets back to MapR-ES or Kafka
    *
    * @param directKafkaInputDStream We can only call this function on the original stream and not the transformations
    * @tparam K
    * @tparam V
    */
  implicit class CanCommitStreamOffsets[K, V](directKafkaInputDStream: DirectKafkaInputDStream[K, V]) {
    def commitOffsetsAsync(): Unit = commitOffsetsAsync(null)

    def commitOffsetsAsync(callback: OffsetCommitCallback): Unit = {
      directKafkaInputDStream.foreachRDD { rdd =>
        val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        directKafkaInputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
      }
    }
  }

}
