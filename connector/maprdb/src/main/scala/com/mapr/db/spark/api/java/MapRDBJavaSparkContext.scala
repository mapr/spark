/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.api.java

import scala.collection.JavaConverters._

import com.mapr.db.spark.RDD.{PairedDocumentRDDFunctions, RDDTYPE}
import com.mapr.db.spark.RDD.api.java.MapRDBJavaRDD
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.{MapRDBUtils, MapRSpark}
import com.mapr.db.spark.writers.{OJAIKey, OJAIValue}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.ojai.DocumentConstants

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


class MapRDBJavaSparkContext(val sparkContext: SparkContext) {

  private var bufferWrites = true
  private var hintUsingIndex: Option[String] = None
  private var queryOptions: Option[Map[String, String]] = None

  def resumeDefaultOptions(): Unit = {
    queryOptions = None
    hintUsingIndex = None
    bufferWrites = true
  }

  def setBufferWrites(bufferWrites: Boolean): Unit = {
    this.bufferWrites = bufferWrites
  }

  def setHintUsingIndex(indexPath: String): Unit = {
    this.hintUsingIndex = Option(indexPath)
  }

  def setQueryOptions(queryOptions: java.util.Map[String, String]): Unit = {
    this.queryOptions = Option(queryOptions.asScala.toMap)
  }


  def setQueryOption(queryOptionKey: String, queryOptionValue: String): Unit = {
    this.queryOptions.getOrElse(Map[String, String]()) + (queryOptionKey -> queryOptionValue)
  }

  def this(javaSparkContext: JavaSparkContext) =
    this(JavaSparkContext.toSparkContext(javaSparkContext))

  def loadFromMapRDB(tableName: String): MapRDBJavaRDD[OJAIDocument] = {
    val rdd = MapRSpark
      .builder()
      .sparkContext(sparkContext)
      .configuration(new Configuration)
      .setTable(tableName)
      .setBufferWrites(bufferWrites)
      .setHintUsingIndex(hintUsingIndex)
      .setQueryOptions(queryOptions.getOrElse(Map[String, String]()))
      .build()
      .toJavaRDD(classOf[OJAIDocument])

    resumeDefaultOptions()

    MapRDBJavaRDD(rdd)
  }

  def loadFromMapRDB[D <: java.lang.Object](
      tableName: String,
      clazz: Class[D]): MapRDBJavaRDD[D] = {
    import scala.reflect._
    implicit val ct: ClassTag[D] = ClassTag(clazz)
    implicit val rddType: RDDTYPE[D] = RDDTYPE.overrideJavaDefaultType[D]
    val rdd = MapRSpark
      .builder()
      .sparkContext(sparkContext)
      .configuration(new Configuration)
      .setTable(tableName)
      .setBufferWrites(bufferWrites)
      .setHintUsingIndex(hintUsingIndex)
      .setQueryOptions(queryOptions.getOrElse(Map[String, String]()))
      .build()
      .toJavaRDD(clazz)

    resumeDefaultOptions()

    MapRDBJavaRDD(rdd)
  }

  import com.mapr.db.spark._

  def saveToMapRDB[D](javaRDD: JavaRDD[D],
                      tableName: String,
                      createTable: Boolean,
                      bulkInsert: Boolean,
                      idField: String): Unit = {

    require(javaRDD != null, "RDD can not be null")

    javaRDD.rdd
      .asInstanceOf[RDD[OJAIValue[D]]]
      .setBufferWrites(bufferWrites)
      .saveToMapRDB(tableName, createTable, bulkInsert, idField)

    resumeDefaultOptions()
  }

  def saveToMapRDB[D](javaRDD: JavaRDD[D],
                      tableName: String,
                      createTable: Boolean,
                      bulkInsert: Boolean): Unit = {
    this.saveToMapRDB(javaRDD,
                      tableName,
                      createTable,
                      bulkInsert,
                      DocumentConstants.ID_KEY)
  }

  def saveToMapRDB[D](javaRDD: JavaRDD[D],
                      tableName: String,
                      createTable: Boolean): Unit = {
    this.saveToMapRDB(javaRDD,
                      tableName,
                      createTable,
                      bulkInsert = false,
                      DocumentConstants.ID_KEY)
  }

  def saveToMapRDB[D](javaRDD: JavaRDD[D], tableName: String): Unit = {
    this.saveToMapRDB(javaRDD,
                      tableName,
                      createTable = false,
                      bulkInsert = false,
                      DocumentConstants.ID_KEY)
  }

  def saveRowRDDToMapRDB(javaRDD: JavaRDD[Row],
                         tableName: String,
                         createTable: Boolean,
                         bulkInsert: Boolean,
                         idField: String): Unit = {

    require(javaRDD != null, "RDD can not be null")
    javaRDD.rdd
      .asInstanceOf[RDD[Row]]
      .setBufferWrites(bufferWrites)
      .saveToMapRDB(tableName, createTable, bulkInsert, idField)
    resumeDefaultOptions()
  }

  def saveRowRDDToMapRDB(javaRDD: JavaRDD[Row],
                         tableName: String,
                         createTable: Boolean,
                         bulkInsert: Boolean): Unit = {
    this.saveRowRDDToMapRDB(javaRDD,
                            tableName,
                            createTable,
                            bulkInsert,
                            DocumentConstants.ID_KEY)
  }

  def saveRowRDDToMapRDB(javaRDD: JavaRDD[Row],
                         tableName: String,
                         createTable: Boolean): Unit = {
    this.saveRowRDDToMapRDB(javaRDD,
                            tableName,
                            createTable,
                            bulkInsert = false,
                            DocumentConstants.ID_KEY)
  }

  def saveRowRDDToMapRDB(javaRDD: JavaRDD[Row], tableName: String): Unit = {
    this.saveRowRDDToMapRDB(javaRDD,
                            tableName,
                            createTable = false,
                            bulkInsert = false,
                            DocumentConstants.ID_KEY)
  }

  def saveToMapRDB[K, V <: AnyRef](javaPairRDD: JavaPairRDD[K, V],
                                   keyClazz: Class[K],
                                   valueClazz: Class[V],
                                   tableName: String,
                                   createTable: Boolean,
                                   bulkInsert: Boolean): Unit = {

    require(javaPairRDD != null, "RDD can not be null")
    require(keyClazz != null, "Key class can not be null")
    require(valueClazz != null, "Value class can not be null")

    import scala.reflect._
    implicit val vct: ClassTag[V] = ClassTag(valueClazz)
    implicit val v: OJAIValue[V] = OJAIValue.overrideDefault[V]

    implicit val ct: ClassTag[K] = ClassTag(keyClazz)
    implicit val f: OJAIKey[K] = MapRDBUtils.getOjaiKey[K]()

    PairedDocumentRDDFunctions(javaPairRDD.rdd).setBufferWrites(bufferWrites)
      .saveToMapRDB(tableName, createTable, bulkInsert)

    resumeDefaultOptions()
  }

  def saveToMapRDB[K, V <: AnyRef](javaPairRDD: JavaPairRDD[K, V],
                                   keyClazz: Class[K],
                                   valueClazz: Class[V],
                                   tableName: String,
                                   createTable: Boolean): Unit = {

    this.saveToMapRDB(javaPairRDD,
                      keyClazz,
                      valueClazz,
                      tableName,
                      createTable,
                      bulkInsert = false)
  }

  def saveToMapRDB[K, V <: AnyRef](javaPairRDD: JavaPairRDD[K, V],
                                   keyClazz: Class[K],
                                   valueClazz: Class[V],
                                   tableName: String): Unit = {

    this.saveToMapRDB(javaPairRDD,
                      keyClazz,
                      valueClazz,
                      tableName,
                      createTable = false,
                      bulkInsert = false)
  }

  def insertToMapRDB[K, V <: AnyRef](javaPairRDD: JavaPairRDD[K, V],
                                   keyClazz: Class[K],
                                   valueClazz: Class[V],
                                   tableName: String,
                                   createTable: Boolean,
                                   bulkInsert: Boolean): Unit = {

    require(javaPairRDD != null, "RDD can not be null")
    require(keyClazz != null, "Key class can not be null")
    require(valueClazz != null, "Value class can not be null")

    import scala.reflect._
    implicit val vct: ClassTag[V] = ClassTag(valueClazz)
    implicit val v: OJAIValue[V] = OJAIValue.overrideDefault[V]

    implicit val ct: ClassTag[K] = ClassTag(keyClazz)
    implicit val f: OJAIKey[K] = MapRDBUtils.getOjaiKey[K]()

    PairedDocumentRDDFunctions(javaPairRDD.rdd)
      .setBufferWrites(bufferWrites)
      .insertToMapRDB(tableName, createTable, bulkInsert)

    resumeDefaultOptions()
  }

  def insertToMapRDB[K, V <: AnyRef](javaPairRDD: JavaPairRDD[K, V],
                                   keyClazz: Class[K],
                                   valueClazz: Class[V],
                                   tableName: String,
                                   createTable: Boolean): Unit = {

    this.insertToMapRDB(javaPairRDD,
      keyClazz,
      valueClazz,
      tableName,
      createTable,
      bulkInsert = false)
  }

  def insertToMapRDB[K, V <: AnyRef](javaPairRDD: JavaPairRDD[K, V],
                                   keyClazz: Class[K],
                                   valueClazz: Class[V],
                                   tableName: String): Unit = {

    this.insertToMapRDB(javaPairRDD,
      keyClazz,
      valueClazz,
      tableName,
      createTable = false,
      bulkInsert = false)
  }

  def insertRowRDDToMapRDB(javaRDD: JavaRDD[Row],
                         tableName: String,
                         createTable: Boolean,
                         bulkInsert: Boolean,
                         idField: String): Unit = {

    require(javaRDD != null, "RDD can not be null")
    javaRDD.rdd
      .asInstanceOf[RDD[Row]]
      .setBufferWrites(bufferWrites)
      .insertToMapRDB(tableName, createTable, bulkInsert, idField)

    resumeDefaultOptions()
  }

  def insertRowRDDToMapRDB(javaRDD: JavaRDD[Row],
                         tableName: String,
                         createTable: Boolean,
                         bulkInsert: Boolean): Unit = {
    this.insertRowRDDToMapRDB(javaRDD,
      tableName,
      createTable,
      bulkInsert,
      DocumentConstants.ID_KEY)
  }

  def insertRowRDDToMapRDB(javaRDD: JavaRDD[Row],
                         tableName: String,
                         createTable: Boolean): Unit = {
    this.insertRowRDDToMapRDB(javaRDD,
      tableName,
      createTable,
      bulkInsert = false,
      DocumentConstants.ID_KEY)
  }

  def insertRowRDDToMapRDB(javaRDD: JavaRDD[Row], tableName: String): Unit = {
    this.insertRowRDDToMapRDB(javaRDD,
      tableName,
      createTable = false,
      bulkInsert = false,
      DocumentConstants.ID_KEY)
  }

  def insertToMapRDB[D](javaRDD: JavaRDD[D],
                      tableName: String,
                      createTable: Boolean,
                      bulkInsert: Boolean,
                      idField: String): Unit = {

    require(javaRDD != null, "RDD can not be null")

    javaRDD.rdd
      .asInstanceOf[RDD[OJAIValue[D]]]
      .setBufferWrites(bufferWrites)
      .insertToMapRDB(tableName, createTable, bulkInsert, idField)

    resumeDefaultOptions()
  }

  def insertToMapRDB[D](javaRDD: JavaRDD[D],
                      tableName: String,
                      createTable: Boolean,
                      bulkInsert: Boolean): Unit = {
    this.insertToMapRDB(javaRDD,
      tableName,
      createTable,
      bulkInsert,
      DocumentConstants.ID_KEY)
  }

  def insertToMapRDB[D](javaRDD: JavaRDD[D],
                      tableName: String,
                      createTable: Boolean): Unit = {
    this.insertToMapRDB(javaRDD,
      tableName,
      createTable,
      bulkInsert = false,
      DocumentConstants.ID_KEY)
  }

  def insertToMapRDB[D](javaRDD: JavaRDD[D], tableName: String): Unit = {
    this.insertToMapRDB(javaRDD,
      tableName,
      createTable = false,
      bulkInsert = false,
      DocumentConstants.ID_KEY)
  }
}
