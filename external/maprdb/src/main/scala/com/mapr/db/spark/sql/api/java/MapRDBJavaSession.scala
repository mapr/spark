/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql.api.java

import scala.collection.JavaConverters._

import com.mapr.db.spark.sql.{GenerateSchema, SingleFragmentOption}
import com.mapr.db.spark.utils.MapRSpark
import org.ojai.DocumentConstants

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.types.StructType

class MapRDBJavaSession(spark: SparkSession) {

  private var bufferWrites = true
  private var hintUsingIndex: Option[String] = None
  private var queryOptions = Map[String, String]()

  def setBufferWrites(bufferWrites: Boolean): Unit = {
    this.bufferWrites = bufferWrites
  }

  def setHintUsingIndex(indexPath: String): Unit = {
    this.hintUsingIndex = Option(indexPath)
  }

  def setQueryOptions(queryOptions: java.util.Map[String, String]): Unit = {
    this.queryOptions = queryOptions.asScala.toMap
  }

  def setQueryOption(queryOptionKey: String, queryOptionValue: String): Unit = {
    this.queryOptions += (queryOptionKey -> queryOptionValue)
  }

  private def resumeDefaultOptions(): Unit = {
    queryOptions = Map[String, String]()
    hintUsingIndex = None
    bufferWrites = true
  }

  def loadFromMapRDB(tableName: String): DataFrame = {
    loadFromMapRDB(tableName, null, GenerateSchema.SAMPLE_SIZE)
  }

  def loadFromMapRDB(tableName: String, schema: StructType): DataFrame = {
    loadFromMapRDB(tableName, schema, GenerateSchema.SAMPLE_SIZE)
  }

  def loadFromMapRDB(tableName: String,
                     schema: StructType,
                     sampleSize: Double): DataFrame = {
    val reader = spark.read
      .format("com.mapr.db.spark.sql")
      .schema(schema)
      .option("tablePath", tableName)
      .option("sampleSize", sampleSize)
      .option("bufferWrites", bufferWrites)
      .option("hintUsingIndex", hintUsingIndex.orNull)
      .options(queryOptions)

    resumeDefaultOptions()

    reader.load()
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
    val reader = spark.read
      .format("com.mapr.db.spark.sql")
      .schema(schema)
      .option("tablePath", tableName)
      .option("sampleSize", sampleSize)
      .option("bufferWrites", bufferWrites)
      .option("hintUsingIndex", hintUsingIndex.orNull)
      .options(queryOptions)

    resumeDefaultOptions()

    reader.load()
      .as(encoder)
  }

  def lookupFromMapRDB(tableName: String): DataFrame = {
    lookupFromMapRDB(tableName, null, GenerateSchema.SAMPLE_SIZE)
  }

  def lookupFromMapRDB(tableName: String, schema: StructType): DataFrame = {
    lookupFromMapRDB(tableName, schema, GenerateSchema.SAMPLE_SIZE)
  }

  def lookupFromMapRDB(tableName: String,
                      schema: StructType,
                      sampleSize: Double): DataFrame = {
    val reader = spark.read
      .format("com.mapr.db.spark.sql")
      .schema(schema)
      .option("tablePath", tableName)
      .option("sampleSize", sampleSize)
      .option("bufferWrites", bufferWrites)
      .option("hintUsingIndex", hintUsingIndex.orNull)
      .options(queryOptions + (SingleFragmentOption -> "true"))

    resumeDefaultOptions()

    reader.load()
  }

  def lookupFromMapRDB(tableName: String, sampleSize: Double): DataFrame = {
    lookupFromMapRDB(tableName, null, sampleSize)
  }

  def lookupFromMapRDB[T <: java.lang.Object](tableName: String,
                                            clazz: Class[T]): Dataset[T] = {
    lookupFromMapRDB(tableName, null, GenerateSchema.SAMPLE_SIZE, clazz)
  }

  def lookupFromMapRDB[T <: java.lang.Object](tableName: String,
                                            schema: StructType,
                                            clazz: Class[T]): Dataset[T] = {
    lookupFromMapRDB(tableName, schema, GenerateSchema.SAMPLE_SIZE, clazz)
  }

  def lookupFromMapRDB[T <: java.lang.Object](tableName: String,
                                            sampleSize: Double,
                                            clazz: Class[T]): Dataset[T] = {
    lookupFromMapRDB(tableName, null, sampleSize, clazz)
  }

  def lookupFromMapRDB[T <: java.lang.Object](tableName: String,
                                              schema: StructType,
                                              sampleSize: Double,
                                              clazz: Class[T]): Dataset[T] = {

    val encoder = Encoders.bean(clazz)
    val reader = spark.read
      .format("com.mapr.db.spark.sql")
      .schema(schema)
      .option("tablePath", tableName)
      .option("sampleSize", sampleSize)
      .option("bufferWrites", bufferWrites)
      .option("hintUsingIndex", hintUsingIndex.orNull)
      .options(queryOptions + (SingleFragmentOption -> "true"))

    resumeDefaultOptions()

    reader.load()
      .as(encoder)
  }

  def saveToMapRDB[T](ds: Dataset[T],
                      tableName: String,
                      idFieldPath: String,
                      createTable: Boolean,
                      bulkInsert: Boolean): Unit = {
    MapRSpark.save(ds, tableName, idFieldPath, createTable, bulkInsert, bufferWrites)
    resumeDefaultOptions()
  }

  def saveToMapRDB(df: DataFrame, tableName: String): Unit =
    saveToMapRDB(df, tableName, DocumentConstants.ID_KEY, false, false)

  def saveToMapRDB(df: DataFrame, tableName: String, createTable: Boolean): Unit =
    saveToMapRDB(df, tableName, DocumentConstants.ID_KEY, createTable, false)

  def insertToMapRDB[T](ds: Dataset[T],
                        tableName: String,
                        idFieldPath: String,
                        createTable: Boolean,
                        bulkInsert: Boolean): Unit = {
    MapRSpark.insert(ds, tableName, idFieldPath, createTable, bulkInsert, bufferWrites)
    resumeDefaultOptions()
  }

  def insertToMapRDB[T](ds: Dataset[T], tableName: String): Unit =
    insertToMapRDB(ds, tableName, DocumentConstants.ID_KEY, false, false)

  def insertToMapRDB[T](ds: Dataset[T], tableName: String, createTable: Boolean): Unit =
    insertToMapRDB(ds, tableName, DocumentConstants.ID_KEY, createTable, false)

}
