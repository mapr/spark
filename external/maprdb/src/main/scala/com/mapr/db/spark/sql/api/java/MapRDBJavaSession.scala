/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql.api.java

import com.mapr.db.spark.sql.GenerateSchema
import com.mapr.db.spark.utils.MapRSpark
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.ojai.DocumentConstants

class MapRDBJavaSession(spark: SparkSession) {

  def loadFromMapRDB(tableName: String): DataFrame = {
    loadFromMapRDB(tableName, null, GenerateSchema.SAMPLE_SIZE)
  }

  def loadFromMapRDB(tableName: String, schema: StructType): DataFrame = {
    loadFromMapRDB(tableName, schema, GenerateSchema.SAMPLE_SIZE)
  }

  def loadFromMapRDB(tableName: String,
                     schema: StructType,
                     sampleSize: Double): DataFrame = {
    spark.read
      .format("com.mapr.db.spark.sql")
      .schema(schema)
      .option("tableName", tableName)
      .option("sampleSize", sampleSize)
      .load()
  }

  def loadFromMapRDB(tableName: String, sampleSize: Double): DataFrame = {
    loadFromMapRDB(tableName, null, sampleSize)
  }

  def loadFromMapRDB[T <: java.lang.Object](tableName: String,
                                            clazz: Class[T]): Dataset[T] = {
    loadFromMapRDB(tableName, null, GenerateSchema.SAMPLE_SIZE, clazz)
  }

  def loadFromMapRDB[T <: java.lang.Object](tableName: String,
                                            schema: StructType,
                                            clazz: Class[T]): Dataset[T] = {
    loadFromMapRDB(tableName, schema, GenerateSchema.SAMPLE_SIZE, clazz)
  }

  def loadFromMapRDB[T <: java.lang.Object](tableName: String,
                                            sampleSize: Double,
                                            clazz: Class[T]): Dataset[T] = {
    loadFromMapRDB(tableName, null, sampleSize, clazz)
  }

  def loadFromMapRDB[T <: java.lang.Object](tableName: String,
                                            schema: StructType,
                                            sampleSize: Double,
                                            clazz: Class[T]): Dataset[T] = {

    val encoder = Encoders.bean(clazz)
    spark.read
      .format("com.mapr.db.spark.sql")
      .schema(schema)
      .option("tableName", tableName)
      .option("sampleSize", sampleSize)
      .load()
      .as(encoder)
  }

  def saveToMapRDB[T](ds: Dataset[T],
                      tableName: String,
                      idFieldPath: String,
                      createTable: Boolean,
                      bulkInsert: Boolean): Unit =
    MapRSpark.save(ds, tableName, idFieldPath, createTable, bulkInsert)

  def saveToMapRDB(df: DataFrame, tableName: String): Unit =
    saveToMapRDB(df, tableName, DocumentConstants.ID_KEY, false, false)

  def saveToMapRDB(df: DataFrame, tableName: String, createTable: Boolean): Unit =
    saveToMapRDB(df, tableName, DocumentConstants.ID_KEY, createTable, false)

  def insertToMapRDB[T](ds: Dataset[T],
                        tableName: String,
                        idFieldPath: String,
                        createTable: Boolean,
                        bulkInsert: Boolean): Unit =
    MapRSpark.insert(ds, tableName, idFieldPath, createTable, bulkInsert)

  def insertToMapRDB[T](ds: Dataset[T], tableName: String): Unit =
    insertToMapRDB(ds, tableName, DocumentConstants.ID_KEY, false, false)

  def insertToMapRDB[T](ds: Dataset[T], tableName: String, createTable: Boolean): Unit =
    insertToMapRDB(ds, tableName, DocumentConstants.ID_KEY, createTable, false)

}
