/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import scala.language.existentials
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import com.mapr.db.impl.{ConditionImpl, IdCodec}
import com.mapr.db.spark.RDD.partition.MaprDBPartition
import com.mapr.db.spark.RDD.partitioner.MapRDBPartitioner
import com.mapr.db.spark.condition._
import com.mapr.db.spark.configuration.SerializableConfiguration
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.DefaultClass.DefaultType
import com.mapr.db.spark.utils.MapRSpark
import org.ojai.{Document, Value}
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

private[spark] class MapRDBTableScanRDD[T: ClassTag](
    @transient sparkSession: SparkSession,
    @transient sc: SparkContext,
    cnf: Broadcast[SerializableConfiguration],
    columns: Seq[String],
    val tableName: String,
    val condition: DBQueryCondition,
    val beanClass: Class[T])(implicit e: T DefaultType OJAIDocument,
                             reqType: RDDTYPE[T])
    extends MapRDBBaseRDD[T](sc, tableName, condition, beanClass, columns) {

  @transient private lazy val table = DBClient().getTable(tableName)
  @transient private lazy val tabletinfos =
    if (condition == null || condition.condition.isEmpty) {
      DBClient().getTabletInfos(tableName)
    } else DBClient().getTabletInfos(tableName, condition.condition)
  @transient private lazy val getSplits: Seq[Value] = {
    val keys = tabletinfos.map(
      tableinfo =>
        IdCodec.decode(
          tableinfo.getCondition
            .asInstanceOf[ConditionImpl]
            .getRowkeyRanges
            .get(0)
            .getStopRow))
    keys.dropRight(1)
  }

  private def getPartitioner: Partitioner = {
    if (getSplits.isEmpty) {
      null
    } else if (getSplits(0).getType == Value.Type.STRING) {
      MapRDBPartitioner(getSplits.map(_.getString))
    } else {
      MapRDBPartitioner(getSplits.map(_.getBinary))
    }
  }

  def toDF[T <: Product: TypeTag](): DataFrame = maprspark[T]()

  def maprspark[T <: Product: TypeTag](): DataFrame = {
    MapRSpark.builder
      .sparkSession(sparkSession)
      .configuration()
      .sparkContext(sparkSession.sparkContext)
      .setDBCond(condition)
      .setTable(tableName)
      .setColumnProjection(Option(columns))
      .build
      .toDF[T]()
  }

  override val partitioner: Option[Partitioner] = Option(getPartitioner)

  override type Self = MapRDBTableScanRDD[T]

  override def getPartitions: Array[Partition] = {
    val splits = tabletinfos.zipWithIndex.map(a => {
      val tabcond = a._1.getCondition
      MaprDBPartition(a._2,
                      tableName,
                      a._1.getLocations,
                      DBClient().getEstimatedSize(a._1),
                      DBQueryCondition(tabcond)).asInstanceOf[Partition]
    })
    logDebug("Partitions for the table:" + tableName + " are " + splits)
    splits.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    logDebug(
      "Preferred Locations: " + split.asInstanceOf[MaprDBPartition].locations)
    split.asInstanceOf[MaprDBPartition].locations
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val cd = split.asInstanceOf[MaprDBPartition].cond.condition
    var combinedCond = DBClient().newCondition()
    var isFullTableScan = true

    if (cd != null && !cd.isEmpty) {
      combinedCond.condition(cd)
      isFullTableScan = false
    }

    if (condition != null && !condition.condition.isEmpty)
      if (isFullTableScan) combinedCond.condition(condition.condition)
      else {
        combinedCond = DBClient()
          .newCondition()
          .and()
          .condition(condition.condition)
          .condition(cd)
          .close()
      }

    logDebug("Condition applied during table.find:" + combinedCond.toString)

    var itrs: java.util.Iterator[Document] = null
    if (columns != null) {
      logDebug("Columns projected from table:" + columns)
      itrs = table.find(combinedCond.build(), columns.toArray: _*).iterator()
    } else {
      itrs = table.find(combinedCond.build()).iterator()
    }
    val ojaiCursor = reqType.getValue(itrs, beanClass)

    context.addTaskCompletionListener((ctx: TaskContext) => {
      logDebug("Task completed")
    })
    ojaiCursor
  }

  override def copy(tblName: String = tableName,
                    columns: Seq[String] = columns,
                    cnd: DBQueryCondition = condition,
                    bclass: Class[T] = beanClass): Self =
    new MapRDBTableScanRDD[T](sparkSession,
                              sc,
                              cnf,
                              columns,
                              tblName,
                              cnd,
                              bclass)
}

object MapRDBTableScanRDD {
  def apply[T: ClassTag](
      sparkSession: SparkSession,
      sc: SparkContext,
      cnf: Broadcast[SerializableConfiguration],
      tableName: String,
      columns: Seq[String],
      cond: DBQueryCondition,
      beanClass: Class[T])(implicit f: RDDTYPE[T]): MapRDBTableScanRDD[T] = {

    new MapRDBTableScanRDD[T](sparkSession,
                              sc = sc,
                              cnf,
                              columns,
                              tableName = tableName,
                              cond,
                              beanClass)
  }
}
