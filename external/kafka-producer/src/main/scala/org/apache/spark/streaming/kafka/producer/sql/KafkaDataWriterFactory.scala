package org.apache.spark.streaming.kafka.producer.sql

import java.util.concurrent.Future
import java.util.concurrent.Future

import org.apache.spark.streaming.kafka.producer.sql.CommittedIds
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.streaming.kafka.producer.ProducerConf
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.util.parsing.json.{JSONArray, JSONObject}

private class KafkaDataWriterFactory(topic: String, schema: StructType) extends DataWriterFactory[InternalRow] {

  @transient private lazy val producerConf = new ProducerConf(
    bootstrapServers = "".split(",").toList)

  @transient private lazy val producer = new KafkaProducer[String, String](producerConf.asJMap())

  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = new DataWriter[InternalRow] with Logging {

    private val writtenIds = scala.collection.mutable.ListBuffer.empty[Future[RecordMetadata]]

    log.info(s"PROCESSING PARTITION ID: $partitionId ; TASK ID: $taskId")

    override def write(record: InternalRow): Unit = {
      val data = record.toSeq(schema).toList

      val map = schema.fields.zipWithIndex
        .map { case (field, idx) => (field.name, data(idx)) }
        .toMap

      val json = toJson(map)

      val task = producer.send(new ProducerRecord(topic, json.toString))

      writtenIds.append(task)

    }


    override def commit(): WriterCommitMessage = {
      val meta = writtenIds.map(_.get())

      writtenIds.clear()
      CommittedIds(partitionId, meta.map(_.offset().toString).toSet)
    }

    override def abort(): Unit = writtenIds.map(_.cancel(true))

    private def toJson(arr: List[Any]): JSONArray = {
      JSONArray(arr.map {
        case (innerMap: Map[String, Any]) => toJson(innerMap)
        case (innerArray: List[Any]) => toJson(innerArray)
        case (other) => other
      })
    }

    private def toJson(map: Map[String, Any]): JSONObject = {
      JSONObject(map.map {
        case (key, innerMap: Map[String, Any]) =>
          (key, toJson(innerMap))
        case (key, innerArray: List[Any]) =>
          (key, toJson(innerArray))
        case (key, other) =>
          (key, other)
      })
    }
  }
}
