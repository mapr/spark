package org.apache.spark.streaming.kafka.producer.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.types.StructType


private class KafkaDataSourceWriter(topic: String, schema: StructType) extends Logging {

  private var globallyCommittedIds = List.empty[String]

  def createWriterFactory(): KafkaDataWriterFactory = new KafkaDataWriterFactory(topic, schema)

  def commit(messages: Array[WriterCommitMessage]): Unit = {

    val ids = messages.foldLeft(Set.empty[String]) { case (acc, CommittedIds(partitionId, partitionIds)) =>
      log.info(s"PARTITION $partitionId HAS BEEN CONFIRMED BY DRIVER")

      acc ++ partitionIds
    }

    // Let's make sure this is thread-safe
    globallyCommittedIds = this.synchronized {
      globallyCommittedIds ++ ids
    }
  }

  def abort(messages: Array[WriterCommitMessage]): Unit = {
    log.info("JOB BEING ABORTED")
  }
}