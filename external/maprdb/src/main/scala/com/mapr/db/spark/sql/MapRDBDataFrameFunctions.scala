/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import com.mapr.db.spark.sql.ojai.{JoinType, OJAISparkPartitionReader}
import com.mapr.db.spark.sql.ojai.OJAISparkPartitionReader.Cell
import com.mapr.db.spark.utils.{LoggingTrait, MapRSpark}
import org.ojai.DocumentConstants

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

private[spark] case class MapRDBDataFrameFunctions(@transient df: DataFrame,
                                                   bufferWrites: Boolean = true)
  extends LoggingTrait {

  def setBufferWrites(bufferWrites: Boolean): MapRDBDataFrameFunctions =
    MapRDBDataFrameFunctions(df, bufferWrites)

  def saveToMapRDB(tableName: String,
                   idFieldPath: String = DocumentConstants.ID_KEY,
                   createTable: Boolean = false,
                   bulkInsert: Boolean = false): Unit =
    MapRSpark.save(df, tableName, idFieldPath, createTable, bulkInsert, bufferWrites)

  def insertToMapRDB(tableName: String,
                     idFieldPath: String = DocumentConstants.ID_KEY,
                     createTable: Boolean = false,
                     bulkInsert: Boolean = false): Unit =
    MapRSpark.insert(df, tableName, idFieldPath, createTable, bulkInsert, bufferWrites)

  def deleteFromMapRDB(tableName: String,
                       idFieldPath: String = DocumentConstants.ID_KEY): Unit =
    MapRSpark.delete(df, tableName, idFieldPath, bufferWrites)

  def joinWithMapRDBTable(table: String,
                          schema: StructType,
                          left: String,
                          right: String,
                          joinType: JoinType,
                          concurrentQueries: Int = 20)(implicit session: SparkSession): DataFrame = {

    val columnDataType = schema.fields(schema.fieldIndex(right)).dataType

    val documents = df
      .select(left)
      .distinct()
      .rdd
      .mapPartitions { partition =>
        if (partition.isEmpty) {
          List.empty.iterator
        } else {

          val partitionCellIterator = partition.map(row => Cell(row.get(0), columnDataType))

          OJAISparkPartitionReader
            .groupedPartitionReader(concurrentQueries)
            .readFrom(partitionCellIterator, table, schema, right)
        }
      }

    import session.implicits._

    import org.apache.spark.sql.functions._

    val rightDF = session.read.schema(schema).json(documents.toDS())

    df.join(rightDF, col(left) === col(right), joinType.toString)
  }

  def joinWithMapRDBTable(maprdbTable: String,
                          schema: StructType,
                          left: String,
                          right: String)(implicit session: SparkSession): DataFrame =
    joinWithMapRDBTable(maprdbTable, schema, left, right, JoinType.inner)
}
