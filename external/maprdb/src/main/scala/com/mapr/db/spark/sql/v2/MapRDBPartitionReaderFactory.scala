package com.mapr.db.spark.sql.v2


import com.mapr.db.spark.utils.LoggingTrait

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class MapRDBPartitionReaderFactory(
      schema: StructType,
      tablePath: String,
      hintedIndexes: Array[String],
      filters: Array[Filter])
    extends PartitionReaderFactory
      with LoggingTrait {

  /**
   * Returns a row-based partition reader to read data from the given {@link InputPartition}.
   *
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[MapRDBInputPartition])

    val mapRDBPartition = partition.asInstanceOf[MapRDBInputPartition]
    new MapRDBPartitionReader(
      tablePath,
      filters,
      schema,
      mapRDBPartition,
      hintedIndexes
    )
  }
}
