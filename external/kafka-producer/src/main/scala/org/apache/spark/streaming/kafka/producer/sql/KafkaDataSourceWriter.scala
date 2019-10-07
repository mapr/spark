package org.apache.spark.streaming.kafka.producer.sql

import java.util.concurrent.Future

import org.apache.spark.streaming.kafka.producer.sql.CommittedIds
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.streaming.kafka.producer.ProducerConf
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


private class KafkaDataSourceWriter(topic: String, schema: StructType) extends DataSourceWriter with Logging {

  private var globallyCommittedIds = List.empty[String]

  override def createWriterFactory(): DataWriterFactory[InternalRow] = new KafkaDataWriterFactory(topic, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

    val ids = messages.foldLeft(Set.empty[String]) { case (acc, CommittedIds(partitionId, partitionIds)) =>
      log.info(s"PARTITION $partitionId HAS BEEN CONFIRMED BY DRIVER")

      acc ++ partitionIds
    }

    // Let's make sure this is thread-safe
    globallyCommittedIds = this.synchronized {
      globallyCommittedIds ++ ids
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    log.info("JOB BEING ABORTED")
  }
}