/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.api.java

import com.mapr.db.spark.RDD.api.java.MapRDBJavaRDD
import com.mapr.db.spark.RDD.{PairedDocumentRDDFunctions, RDDTYPE}
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.{MapRDBUtils, MapRSpark}
import com.mapr.db.spark.writers.{OJAIKey, OJAIValue}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.ojai.DocumentConstants

class MapRDBJavaSparkContext(val sparkContext: SparkContext) {

  def this(javaSparkContext: JavaSparkContext) =
    this(JavaSparkContext.toSparkContext(javaSparkContext))

  def loadFromMapRDB(tableName: String): MapRDBJavaRDD[OJAIDocument] = {
    val rdd = MapRSpark
      .builder()
      .sparkContext(sparkContext)
      .configuration(new Configuration)
      .setTable(tableName)
      .build()
      .toJavaRDD(classOf[OJAIDocument])

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
      .build()
      .toJavaRDD(clazz)

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
      .saveToMapRDB(tableName, createTable, bulkInsert, idField)
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
      .saveToMapRDB(tableName, createTable, bulkInsert, idField)
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

    PairedDocumentRDDFunctions(javaPairRDD.rdd)
      .saveToMapRDB(tableName, createTable, bulkInsert)
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
      .insertToMapRDB(tableName, createTable, bulkInsert)
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
      .insertToMapRDB(tableName, createTable, bulkInsert, idField)
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
      .insertToMapRDB(tableName, createTable, bulkInsert, idField)
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
