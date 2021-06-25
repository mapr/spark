package com.mapr.db.spark.sql.v2

import scala.collection.JavaConverters._
import scala.util.Random

import com.mapr.db.spark.utils.LoggingTrait
import org.ojai.store.{DocumentStore, DriverManager}
import org.ojai.store.QueryCondition.Op

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class MapRDBScan(
      schema: StructType,
      tablePath: String,
      hintedIndexes: Array[String],
      filters: Array[Filter],
      readersPerTablet: Int)
    extends Scan
      with Batch
      with LoggingTrait {

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   */
  override def readSchema(): StructType = schema

  /**
   * Returns a list of {@link InputPartition input partitions}. Each {@link InputPartition}
   * represents a data split that can be processed by one Spark task. The number of input
   * partitions returned here is the same as the number of RDD partitions this scan outputs.
   * <p>
   * If the {@link Scan} supports filter pushdown, this Batch is likely configured with a filter
   * and is responsible for creating splits for that filter, which is not a full scan.
   * </p>
   * <p>
   * This method will be called only once during a data source scan, to launch one Spark job.
   * </p>
   */
  override def planInputPartitions(): Array[InputPartition] =
    if (readersPerTablet == 1) {
      singleReaderPartitions()
    } else {
      multiReaderPartitions()
    }

  private def singleReaderPartitions(): Array[InputPartition] = {
    com.mapr.db.MapRDB
      .getTable(tablePath)
      .getTabletInfos
      .zipWithIndex
      .map { case (descriptor, idx) =>
        MapRDBInputPartition(idx, descriptor.getLocations, descriptor.getCondition.asJsonString)
          .asInstanceOf[InputPartition]
      }
  }

  private def multiReaderPartitions(): Array[InputPartition] = {
    val connection = DriverManager.getConnection("ojai:mapr:")
    val store: DocumentStore = connection.getStore(tablePath)

    val partitions = com.mapr.db.MapRDB
      .getTable(tablePath)
      .getTabletInfos
      .flatMap { tablet =>
        val query = connection
          .newQuery()
          .where(tablet.getCondition)
          .select("_id")
          .build()

        val ids = store.find(query)
        val partition = ids.asScala.toList
        val partitionSize = partition.size

        logInfo(s"READER SIZE == $partitionSize")

        partition
          .grouped((partitionSize / readersPerTablet) + 1)
          .filter(_.nonEmpty)
          .map(group => (group.head.getIdString, group.last.getIdString))
          .map { range =>

            val lowerBound = connection.newCondition().is("_id", Op.GREATER_OR_EQUAL, range._1)
            val upperBound = connection.newCondition().is("_id", Op.LESS_OR_EQUAL, range._2)

            val cond = connection
              .newCondition()
              .and()
              .condition(lowerBound.build())
              .condition(upperBound.build())
              .close()
              .build()
              .asJsonString()

            MapRDBInputPartition(Random.nextInt(), tablet.getLocations, cond).asInstanceOf[InputPartition]
          }
      }

    logInfo(s"CREATING ${partitions.length} READERS")

    partitions
  }

  /**
   * Returns a factory to create a {@link PartitionReader} for each {@link InputPartition}.
   */
  override def createReaderFactory(): PartitionReaderFactory =
    MapRDBPartitionReaderFactory(schema, tablePath, hintedIndexes, filters)

  override def toBatch: Batch = this
}
