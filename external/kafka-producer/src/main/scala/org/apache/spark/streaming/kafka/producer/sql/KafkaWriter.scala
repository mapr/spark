package org.apache.spark.streaming.kafka.producer.sql

import java.util.Optional

import org.apache.parquet.hadoop.api.WriteSupport

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

class KafkaWriter extends WriteSupport with Logging {
  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {

    val stream = options.get("path").get()

    java.util.Optional.of(new KafkaDataSourceWriter(stream, schema))
  }
}
