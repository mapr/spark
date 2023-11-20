/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.streaming.sink

import com.mapr.db.spark._
import com.mapr.db.spark.streaming.MapRDBSourceConfig
import org.ojai.DocumentConstants
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.{Command, LocalRelation, LogicalPlan, Union}
import org.apache.spark.sql.execution.streaming.Sink

private[streaming] class MapRDBSink(parameters: Map[String, String]) extends Sink with Logging {

  @volatile private var latestBatchId = -1L

  override def toString(): String = "MapRDBSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {

      val tablePath = parameters.get(MapRDBSourceConfig.TablePathOption)
      require(tablePath.isDefined,
        s"'${MapRDBSourceConfig.TablePathOption}' option must be defined")

      val bufferWrites = parameters.getOrElse(MapRDBSourceConfig.BufferWrites, "true").toBoolean

      val idFieldPath = parameters
        .getOrElse(MapRDBSourceConfig.IdFieldPathOption, DocumentConstants.ID_KEY)
      val createTable = parameters
        .getOrElse(MapRDBSourceConfig.CreateTableOption, "false").toBoolean
      val bulkInsert = parameters.getOrElse(MapRDBSourceConfig.BulkModeOption, "false").toBoolean

      val logicalPlan: LogicalPlan = {
        // For various commands (like DDL) and queries with side effects, we force query execution
        // to happen right away to let these side effects take place eagerly.
        data.queryExecution.analyzed match {
          case c: Command =>
            LocalRelation(c.output, data.queryExecution.executedPlan.executeCollect())
          case u@Union(children, _, _) if children.forall(_.isInstanceOf[Command]) =>
            LocalRelation(u.output, data.queryExecution.executedPlan.executeCollect())
          case _ =>
            data.queryExecution.analyzed
        }
      }

      val encoder = ExpressionEncoder(data.schema)
        .resolveAndBind(logicalPlan.output, data.sparkSession.sessionState.analyzer)
        .createDeserializer()

      data.queryExecution.toRdd
        .map(encoder)
        .setBufferWrites(bufferWrites)
        .saveToMapRDB(tablePath.get, createTable, bulkInsert, idFieldPath)

      latestBatchId = batchId
    }
  }

}
