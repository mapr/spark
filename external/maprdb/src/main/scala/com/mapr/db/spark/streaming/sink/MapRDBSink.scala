package com.mapr.db.spark.streaming.sink


import com.mapr.db.spark.sql._
import com.mapr.db.spark.streaming.MapRDBSourceConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import org.ojai.DocumentConstants

private[streaming] class MapRDBSink(parameters: Map[String, String]) extends Sink with Logging {

  @volatile private var latestBatchId = -1L

  override def toString(): String = "MapRDBSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {

      val tablePath = parameters.get(MapRDBSourceConfig.TablePathOption)
      require(tablePath.isDefined, s"'${MapRDBSourceConfig.TablePathOption}' option must be defined")

      val idFieldPath = parameters.getOrElse(MapRDBSourceConfig.IdFieldPathOption, DocumentConstants.ID_KEY)
      val createTable = parameters.getOrElse(MapRDBSourceConfig.CreateTableOption, "false").toBoolean
      val bulkInsert = parameters.getOrElse(MapRDBSourceConfig.BulkModeOption, "false").toBoolean

      data.saveToMapRDB(tablePath.get, idFieldPath, createTable, bulkInsert)

      latestBatchId = batchId
    }
  }
}
