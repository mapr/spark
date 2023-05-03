/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark

import scala.reflect.{ClassTag, _}

import com.mapr.db.spark.RDD.{MapRDBTableScanRDD, RDDTYPE}
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.DefaultClass.DefaultType
import com.mapr.db.spark.utils.MapRSpark
import org.apache.spark.SparkContext

case class SparkContextFunctions(@transient sc: SparkContext,
                                 bufferWrites: Boolean = true,
                                 hintUsingIndex: Option[String] = None,
                                 queryOptions: Map[String, String] = Map[String, String]())
  extends Serializable {

  def setBufferWrites(bufferWrites: Boolean): SparkContextFunctions =
    SparkContextFunctions(sc, bufferWrites, hintUsingIndex, queryOptions)

  def setHintUsingIndex(indexPath: String): SparkContextFunctions =
    SparkContextFunctions(sc, bufferWrites, Option(indexPath), queryOptions)

  def setQueryOptions(queryOptions: Map[String, String]): SparkContextFunctions =
    SparkContextFunctions(sc, bufferWrites, hintUsingIndex, queryOptions)

  def setQueryOption(queryOptionKey: String, queryOptionValue: String): SparkContextFunctions =
    SparkContextFunctions(sc, bufferWrites, hintUsingIndex,
      queryOptions + (queryOptionKey -> queryOptionValue))

  /**
    * Spark MapRDB connector specific functions to load json tables as RDD[OJAIDocument]
    *
    * @param tableName name of the table in MapRDB
    * @example val docs = sc.loadMapRDBTable("tablePath")
    */
  def loadFromMapRDB[T: ClassTag](tableName: String)(
    implicit e: T DefaultType OJAIDocument,
    f: RDDTYPE[T]): MapRDBTableScanRDD[T] =
    MapRSpark.builder
      .sparkContext(sc)
      .configuration()
      .setTable(tableName)
      .setBufferWrites(bufferWrites)
      .setHintUsingIndex(hintUsingIndex)
      .setQueryOptions(queryOptions)
      .build()
      .toRDD[T](classTag[T].runtimeClass.asInstanceOf[Class[T]])

}
