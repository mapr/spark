package com.mapr.db.spark.sql.v2

import scala.util.Random

import com.mapr.db.spark.sql.v2.QueryConditionExtensions._
import java.util
import org.ojai.store.{DocumentStore, DriverManager}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

/**
  * MapRDBDataSourceMultiReader creates the corresponding reader to read data from different MapR-DB tablets.
  *
  * @param schema           Schema to be used.
  * @param tablePath        Table to read data from.
  * @param hintedIndexes    List of index hits to be passed to MapR-DB through OJAI.
  * @param readersPerTablet The number of readers that will be created for each MapR-DB tablet. The default is 1.
  *                         In most cases, 1 is more than enough.
  *                         Creating multiple readers per tablet makes the reading process faster, but there is
  *                         a performance hit we need to pay when creating the readers. Normally it works fine for
  *                         very long tables.
  */
class MapRDBDataSourceMultiReader(schema: StructType,
                                  tablePath: String,
                                  hintedIndexes: List[String],
                                  readersPerTablet: Int)
  extends MapRDBDataSourceReader(schema, tablePath, hintedIndexes) {

  import scala.collection.JavaConverters._

  /**
    * Decides if we are creating 1 or multiple readers per tablet.
    *
    * @return
    */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] =
    if (readersPerTablet == 1) {
      super.planInputPartitions()
    } else {
      createReaders.asInstanceOf[List[InputPartition[InternalRow]]].asJava
    }

  /**
    * This function is used to create multiple readers per MapR-DB tablet.
    * There is a performance hit since all the _ids per each tablet need to be read in order to partition the tablet
    * into multiple sub-ranges.
    *
    * @return
    */
  private def createReaders: List[MapRDBDataPartitionReader] = {

    val connection = DriverManager.getConnection("ojai:mapr:")
    val store: DocumentStore = connection.getStore(tablePath)

    val conditions = com.mapr.db.MapRDB
      .getTable(tablePath)
      .getTabletInfos
      .par
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

            val lowerBound = connection.newCondition().field("_id") >= range._1
            val upperBound = connection.newCondition().field("_id") <= range._2

            val cond = connection
              .newCondition()
              .and()
              .condition(lowerBound.build())
              .condition(upperBound.build())
              .close()
              .build()
              .asJsonString()

            MapRDBTabletInfo(Random.nextInt(), tablet.getLocations, cond)
          }
      }

    val factories = conditions.map(createReaderFactory).toList

    logInfo(s"CREATING ${factories.length} READERS")

    factories
  }
}
