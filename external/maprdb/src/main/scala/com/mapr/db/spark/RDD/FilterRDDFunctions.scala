/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import scala.reflect._

import com.mapr.db.spark.condition._
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.field
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.DefaultClass.DefaultType
import com.mapr.db.spark.writers.OJAIKey
import org.ojai.DocumentConstants
import org.ojai.store.DriverManager

import org.apache.spark.rdd.RDD

case class FilterRDDFunctions[K: OJAIKey: quotes](rdd: RDD[K]) {

  val driver = DriverManager.getConnection("ojai:mapr:").getDriver

  def joinWithMapRDB[D: ClassTag](tableName: String, bufferWrites: Boolean = true)(
      implicit e: D DefaultType OJAIDocument,
      reqType: RDDTYPE[D]): RDD[D] = {
    rdd.mapPartitions(partition => {
      val table = DBClient().getTable(tableName, bufferWrites)

      partition.flatMap(item => {
        val condition = field(DocumentConstants.ID_KEY) === item
        reqType.getValue(table.find(
          driver.newQuery().where(condition.build).build
        ).iterator(),
            classTag[D].runtimeClass.asInstanceOf[Class[D]])
      })
    })
  }

  def bulkJoinWithMapRDB[D: ClassTag](tableName: String, bufferWrites: Boolean = true)(
      implicit e: D DefaultType OJAIDocument,
      reqType: RDDTYPE[D]): RDD[D] = {
    rdd.mapPartitions(partition => {
      val table = DBClient().getTable(tableName, bufferWrites)
      var gets = Seq[K]()
      var res = List[D]()

      while (partition.hasNext) {
        gets = gets :+ partition.next
        if (gets.size == 4) {
          val condition = field(DocumentConstants.ID_KEY) in gets
          res = res ++ reqType.getValue(
            table.find(
              driver.newQuery().where(condition.build).build
            ).iterator(),
            classTag[D].runtimeClass.asInstanceOf[Class[D]])
          gets = Seq[K]()
        }
      }

      if (gets.nonEmpty) {
        val condition = field(DocumentConstants.ID_KEY) in gets
        res = res ++ reqType.getValue(
          table.find(
            driver.newQuery().where(condition.build).build
          ).iterator(),
          classTag[D].runtimeClass.asInstanceOf[Class[D]])
        gets = Seq[K]()
      }
      res.iterator
    })
  }
}
