/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import com.mapr.db.spark.field
import com.mapr.db.spark.condition._
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.DefaultClass.DefaultType
import com.mapr.db.spark.writers.OJAIKey
import org.apache.spark.rdd.RDD
import scala.language.implicitConversions
import com.mapr.db.spark.dbclient.DBClient
import scala.reflect._

case class FilterRDDFunctions[K : OJAIKey : quotes](rdd : RDD[K]) {

  def joinWithMapRDB[D : ClassTag](tableName: String)(implicit e: D DefaultType OJAIDocument, reqType: RDDTYPE[D]): RDD[D] = {
    rdd.mapPartitions( partition => {
      val table = DBClient().getTable(tableName)

      partition.flatMap(item => {
        val condition = field("_id") === item
        reqType.getValue(table.find(condition.build).iterator(), classTag[D].runtimeClass.asInstanceOf[Class[D]])
      })
    })
  }

  def bulkJoinWithMapRDB[D : ClassTag](tableName: String)(implicit e: D DefaultType OJAIDocument, reqType: RDDTYPE[D]): RDD[D] = {
    rdd.mapPartitions( partition => {
      val table = DBClient().getTable(tableName)
      var gets = Seq[K]()
      var res = List[D]()

      while (partition.hasNext) {
        gets = gets :+ partition.next
        if (gets.size == 4) {
          val condition = field("_id") in gets
          res = res ++ reqType.getValue(table.find(condition.build).iterator(), classTag[D].runtimeClass.asInstanceOf[Class[D]])
          gets = Seq[K]()
        }
      }

      if (gets.size > 0) {
        val condition = field("_id") in gets
        res = res ++ reqType.getValue(table.find(condition.build).iterator(), classTag[D].runtimeClass.asInstanceOf[Class[D]])
        gets = Seq[K]()
      }
      res.iterator
  })

//  def bulkJoinWithMapRDB[D : ClassTag](tableName: String)(implicit e: D DefaultType OJAIDocument, reqType: RDDTYPE[D]): RDD[D] = {
//    rdd.mapPartitions( partition => {
//      val table = MapRDBImpl.getTable(tableName)
//      val preparedPartitions : Seq[Seq[K]] = partition.foldLeft[Seq[Seq[K]]](Seq[Seq[K]]()) {
//        case (Nil, item) => Seq(Seq(item))
//        case (result, s) => {
//          if (result.last.size < 4) result.dropRight(1) :+ (result.last :+ s)
//          else result :+ Seq(s)
//        }
//      }
//
//      val output : Iterator[D]= preparedPartitions.map(items => {
//        val condition = field("_id") in items
//        reqType.getValue(table.find(condition.build).iterator(), classTag[D].runtimeClass.asInstanceOf[Class[D]])})
//      output })
//      output })
//      return preparedPartitions.asInstanceOf[RDD[D]]


//        .flatMap(items : Seq[K] => {
//        val condition = field("_id") in items
//        reqType.getValue(table.find(condition.build).iterator(), classTag[D].runtimeClass.asInstanceOf[Class[D]])
//      }
//    })
//      partition.foldLeft[List[K](List[Iterator[_]]()){
//        case (Nil, s) => List(List(s))
//        case (result, s) => if (result.last.size < 4) result.dropRight(1) :+ (result.last :+ s)
//                            else result :+ List(s)
//      }.flatMap(items => {
//        val condition = field("_id") in items
//        reqType.getValue(table.find(condition.build).iterator(), classTag[D].runtimeClass.asInstanceOf[Class[D]])
//      })
//    })
  }
}
