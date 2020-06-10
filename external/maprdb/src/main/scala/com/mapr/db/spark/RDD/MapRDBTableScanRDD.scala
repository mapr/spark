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
import org.ojai.store.DriverManager

import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

private[spark] class MapRDBTableScanRDD[T: ClassTag](
    @transient val sparkSession: SparkSession,
    @transient override val sc: SparkContext,
    cnf: Broadcast[SerializableConfiguration],
    columns: Seq[String],
    val tableName: String,
    val bufferWrites: Boolean = true,
    val hintUsingIndex: String,
    val condition: DBQueryCondition,
    val beanClass: Class[T],
    queryOptions: Map[String, String] = Map[String, String]())
                                                    (implicit e: T DefaultType OJAIDocument,
                             reqType: RDDTYPE[T])
    extends MapRDBBaseRDD[T](sc, tableName, condition, beanClass, columns, queryOptions) {

  @transient private lazy val table = DBClient().getTable(tableName, bufferWrites)
  @transient private lazy val tabletinfos =
    if (condition == null || condition.condition.isEmpty) {
      DBClient().getTabletInfos(tableName, bufferWrites)
    } else DBClient().getTabletInfos(tableName, condition.condition, bufferWrites)
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
      .setBufferWrites(bufferWrites)
      .setColumnProjection(Option(columns))
      .setQueryOptions(queryOptions)
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

    val driver = DriverManager.getDriver("ojai:mapr:")
    var query = driver.newQuery()

    if (columns != null) {
      logDebug("Columns projected from table:" + columns)
      query = query.select(columns.toArray: _*)
    }
    if (hintUsingIndex != null) {
      query = query.setOption("ojai.mapr.query.hint-using-index", hintUsingIndex)
    }

    queryOptions
      .filterKeys(k => k.startsWith("ojai.mapr.query")).map(identity)
        .filter(opt => opt._2 != null)
        .map(opt => {
          opt._2.toLowerCase match {
            case "true" => (opt._1, true)
            case "false" => (opt._1, false)
            case _ => opt
          }
        })
        .foreach(opt => query = query.setOption(opt._1, opt._2))

    val itrs: java.util.Iterator[Document] =
      table.find(query.where(combinedCond.build()).build()).iterator()

    val ojaiCursor = reqType.getValue(itrs, beanClass)

    context.addTaskCompletionListener[Unit](_ => logDebug("Task completed"))
    ojaiCursor
  }

  override def copy(tblName: String = tableName,
                    columns: Seq[String] = columns,
                    cnd: DBQueryCondition = condition,
                    bclass: Class[T] = beanClass,
                    queryOptions: Map[String, String] = queryOptions): Self =
    new MapRDBTableScanRDD[T](sparkSession,
                              sc,
                              cnf,
                              columns,
                              tblName,
                              bufferWrites,
                              hintUsingIndex,
                              cnd,
                              bclass,
                              queryOptions)
}

object MapRDBTableScanRDD {
  def apply[T: ClassTag](
      sparkSession: SparkSession,
      sc: SparkContext,
      cnf: Broadcast[SerializableConfiguration],
      tableName: String,
      bufferWrites: Boolean,
      hintUsingIndex: String,
      columns: Seq[String],
      cond: DBQueryCondition,
      beanClass: Class[T],
      queryOptions: Map[String, String])
                        (implicit f: RDDTYPE[T]): MapRDBTableScanRDD[T] = {

    new MapRDBTableScanRDD[T](sparkSession,
                              sc = sc,
                              cnf,
                              columns,
                              tableName = tableName,
                              bufferWrites,
                              hintUsingIndex,
                              cond,
                              beanClass,
                              queryOptions)
  }
}
