/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import com.mapr.db.spark.utils.{LoggingTrait, MapRSpark}
import org.ojai.DocumentConstants

import org.apache.spark.sql.DataFrameWriter

private[spark] case class MapRDBDataFrameWriterFunctions(
    @transient dfw: DataFrameWriter[_], bufferWrites: Boolean = true)
    extends LoggingTrait {

  def setBufferWrites(bufferWrites: Boolean): MapRDBDataFrameWriterFunctions =
    MapRDBDataFrameWriterFunctions(dfw, bufferWrites)

  def saveToMapRDB(tableName: String,
                   idFieldPath: String = DocumentConstants.ID_KEY,
                   bulkInsert: Boolean = false): Unit =
    MapRSpark.save(dfw, tableName, idFieldPath, bulkInsert, bufferWrites)

}
