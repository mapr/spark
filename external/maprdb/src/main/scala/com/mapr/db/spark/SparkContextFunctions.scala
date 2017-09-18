/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark

import utils.DefaultClass.DefaultType
import org.apache.spark.SparkContext
import com.mapr.db.spark.utils.MapRSpark

import scala.reflect.ClassTag
import scala.reflect._
import com.mapr.db.spark.RDD.RDDTYPE
import com.mapr.db.spark.impl.OJAIDocument

case class SparkContextFunctions( @transient val sc: SparkContext) extends Serializable {
  /**
    * Spark MapRDB connector specific functions to load json tables as RDD[OJAIDocument]
    * @param tableName name of the table in MapRDB
    * @example val docs = sc.loadMapRDBTable("tableName")
    */
  def loadFromMapRDB[T: ClassTag](tableName: String)(implicit e: T DefaultType OJAIDocument, f: RDDTYPE[T]) =
    MapRSpark.builder.sparkContext(sc).configuration().setTable(tableName).build().toRDD[T](classTag[T].runtimeClass.asInstanceOf[Class[T]])

}
