/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import com.mapr.db.spark.condition.Predicate
import com.mapr.db.spark.utils.{LoggingTrait, MapRSpark}
import org.apache.spark.sql.{DataFrame, Row}
import org.ojai.store.DocumentMutation

private[spark] case class MapRDBDataFrameFunctions(@transient df: DataFrame) extends LoggingTrait {

  def saveToMapRDB(tableName: String, idFieldPath : String = "_id",
                   createTable: Boolean = false, bulkInsert:Boolean = false): Unit =
          MapRSpark.save(df, tableName,idFieldPath,createTable,bulkInsert)

  def insertToMapRDB(tableName: String, idFieldPath : String = "_id",
                   createTable: Boolean = false, bulkInsert:Boolean = false): Unit =
    MapRSpark.insert(df, tableName,idFieldPath,createTable,bulkInsert)

  def updateToMapRDB(tableName: String, mutation: (Row) => DocumentMutation, getID: (Row) => org.ojai.Value) : Unit =
    MapRSpark.update(df, tableName, mutation, getID)

  def updateToMapRDB(tableName: String, mutation: (Row) => DocumentMutation, getID: (Row) => org.ojai.Value, condition: Predicate) : Unit =
    MapRSpark.update(df, tableName, mutation, getID, condition)
}
