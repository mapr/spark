/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.streaming

import com.mapr.db.spark._
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.utils.LoggingTrait
import com.mapr.db.spark.writers.OJAIValue
import org.apache.spark.SparkContext
import org.ojai.DocumentConstants

import org.apache.spark.streaming.dstream.DStream

class DStreamFunctions[T](dStream: DStream[T])(implicit fv: OJAIValue[T])
    extends Serializable
    with LoggingTrait {

  def sparkContext: SparkContext = dStream.context.sparkContext

  def saveToMapRDB(tableName: String,
                   createTable: Boolean = false,
                   bulkInsert: Boolean = false,
                   idFieldPath: String = DocumentConstants.ID_KEY): Unit = {
    logDebug(
      "DStreamFunctions is called for table: " + tableName + " with bulkinsert flag set: "
        + bulkInsert + " and createTable:" + createTable)

    if (createTable) {
      logDebug("Table:" + tableName + " is created in DStreamFunctions")
      DBClient().createTable(tableName)
    }

    dStream.foreachRDD(rdd =>
      rdd.saveToMapRDB(tableName, false, bulkInsert, idFieldPath))
  }
}
