/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.utils

import scala.reflect._
import scala.reflect.runtime.universe._

import com.mapr.db.impl.ConditionImpl
import com.mapr.db.spark._
import com.mapr.db.spark.RDD.{MapRDBBaseRDD, MapRDBTableScanRDD, RDDTYPE}
import com.mapr.db.spark.condition.DBQueryCondition
import com.mapr.db.spark.configuration.SerializableConfiguration
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.sql.GenerateSchema
import com.mapr.db.spark.sql.utils.MapRSqlUtils
import com.mapr.db.spark.utils.DefaultClass.DefaultType
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.ojai.store.QueryCondition

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

object MapRSpark {

  val defaultSource = "com.mapr.db.spark.sql.DefaultSource"

  def builder(): Builder = new Builder

  def save[D](dataset: Dataset[D],
              tableName: String,
              idFieldPath: String,
              createTable: Boolean,
              bulkInsert: Boolean,
              bufferWrites: Boolean): Unit = {
    val documentRdd = dataset.toDF().rdd.map(MapRSqlUtils.rowToDocument)
    documentRdd.setBufferWrites(bufferWrites).saveToMapRDB(tableName,
                             createTable = createTable,
                             bulkInsert = bulkInsert,
                             idFieldPath = idFieldPath)
  }

  def insert[D](dataset: Dataset[D],
                tableName: String,
                idFieldPath: String,
                createTable: Boolean,
                bulkInsert: Boolean,
                bufferWrites: Boolean): Unit = {
    val documentRdd = dataset.toDF.rdd.map(MapRSqlUtils.rowToDocument)
    documentRdd.setBufferWrites(bufferWrites).insertToMapRDB(tableName,
                               createTable = createTable,
                               bulkInsert = bulkInsert,
                               idFieldPath = idFieldPath)
  }

  def delete[D](dataset: Dataset[D],
                tableName: String,
                idFieldPath: String,
                bufferWrites: Boolean): Unit = {
    val documentRdd = dataset.toDF.rdd.map(MapRSqlUtils.rowToDocument)
    documentRdd.setBufferWrites(bufferWrites).deleteFromMapRDB(tableName,
                               idFieldPath = idFieldPath)
  }

  def save(
      dfw: DataFrameWriter[_],
      tableName: String,
      idFieldPath: String,
      bulkInsert: Boolean,
      bufferWrites: Boolean
  ): Unit = {
    dfw
      .format(defaultSource)
      .option("tablePath", tableName)
      .option("idFieldPath", idFieldPath)
      .option("bulkMode", bulkInsert)
      .option("bufferWrites", bufferWrites)
      .save()
  }

  def load(sc: SparkContext, tableName: String): MapRDBBaseRDD[OJAIDocument] = {
    sc.loadFromMapRDB(tableName)
  }

  class Builder {
    private var sparkctx: Option[SparkContext] = None
    private var sparkSession: Option[SparkSession] = None
    private var condition: Option[QueryCondition] = None
    private var dbcondition: Option[DBQueryCondition] = None
    private var tableName: Option[String] = None
    private var bufferWrites: Option[Boolean] = None
    private var hintUsingIndex: Option[String] = None
    private var sampleSize: Option[String] = None
    private var conf: Option[SerializableConfiguration] = None
    private var beanClass: Option[Class[_]] = None
    private var columnProjection: Option[Seq[String]] = None
    private var queryOptions: Option[Map[String, String]] = None

    def build(): MapRSpark = {
      require(sparkctx.isDefined, "The SparkContext must be set.")
      require(tableName.isDefined, "Source should be set")
      require(conf.isDefined, "Configuration should be set")

      new MapRSpark(sparkSession,
                    sparkctx,
                    conf,
                    dbcondition,
                    tableName,
                    bufferWrites,
                    hintUsingIndex,
                    columnProjection,
                    queryOptions)
    }

    def configuration(conf: Configuration = new Configuration): Builder = {
      val sercnf = new SerializableConfiguration(conf)
      this.conf = Option(sercnf)
      this
    }

    def setQueryOptions(queryOptions: Map[String, String]): Builder = {
      this.queryOptions = Option(queryOptions)
      this
    }

    def sparkContext(ctx: SparkContext): Builder = {
      this.sparkctx = Option(ctx)
      this.sparkSession = Option(
        SparkSession.builder().config(ctx.getConf).getOrCreate())
      this
    }

    def sparkSession(sparkSession: SparkSession): Builder = {
      this.sparkSession = Option(sparkSession)
      this.sparkctx = Option(sparkSession.sparkContext)
      this
    }

    def setCond(cond: Option[QueryCondition]): Builder = {
      this.condition = cond
      this.dbcondition =
        if (cond.isDefined) Option(DBQueryCondition(cond.get)) else None
      this
    }

    def setDBCond(cond: DBQueryCondition): Builder = {
      this.dbcondition = Option(cond)
      this
    }

    def setTable(tableName: String): Builder = {
      this.tableName = Option(tableName)
      this
    }

    def setBufferWrites(bufferWrites: Boolean): Builder = {
      this.bufferWrites = Option(bufferWrites)
      this
    }

    def setHintUsingIndex(indexPath: Option[String]): Builder = {
      this.hintUsingIndex = indexPath
      this
    }

    def setBeanClass(beanClass: Class[_]): Builder = {
      this.beanClass = Option(beanClass)
      this
    }

    def setColumnProjection(columns: Option[Seq[String]]): Builder = {
      this.columnProjection = columns
      this
    }
  }
}

case class MapRSpark(sparkSession: Option[SparkSession],
                     sc: Option[SparkContext],
                     conf: Option[SerializableConfiguration],
                     cond: Option[DBQueryCondition],
                     tableName: Option[String],
                     bufferWrites: Option[Boolean],
                     hintUsingIndex: Option[String],
                     columns: Option[Seq[String]],
                     queryOptions: Option[Map[String, String]]) {

  def toRDD[T: ClassTag](beanClass: Class[T] = null)(
      implicit e: T DefaultType OJAIDocument,
      f: RDDTYPE[T]): MapRDBTableScanRDD[T] = rdd[T](beanClass)

  def toJavaRDD[D: ClassTag](clazz: Class[D])
                            (implicit f: RDDTYPE[D]): MapRDBTableScanRDD[D] =
    toRDD[D](clazz)

  private def rdd[T: ClassTag](beanClass: Class[T])(
      implicit e: T DefaultType OJAIDocument,
      f: RDDTYPE[T]): MapRDBTableScanRDD[T] =
    new MapRDBTableScanRDD[T](sparkSession.get,
                              sc.get,
                              sc.get.broadcast(conf.get),
                              columns.orNull,
                              tableName.get,
                              bufferWrites.get,
                              hintUsingIndex.orNull,
                              cond.orNull,
                              beanClass,
                              queryOptions.get)

  def toDataFrame(schema: StructType, sampleSize: Double,
                  bufferWrites: Boolean): DataFrame = {
    val reader: DataFrameReader = sparkSession.get.read
      .format("com.mapr.db.spark.sql")
      .schema(schema)
      .option("tablePath", this.tableName.get)
      .option("sampleSize", sampleSize)
      .option("bufferWrites", bufferWrites)

    queryOptions.get.foreach(option => reader.option(option._1, option._2))

      if (cond.isDefined) {
        reader.option("QueryCondition",
        new String(
          cond.get.condition
            .asInstanceOf[ConditionImpl]
            .getDescriptor
            .getSerialized
            .array,
          "ISO-8859-1"
        ))
      }

      if (columns.isDefined) {
      reader.option(
        "ColumnProjection",
        columns
          .getOrElse(Seq(""))
          .reduce[String]((str1, str2) => str1 + "," + str2))
    }

    reader.load()
  }



  def toOJAIDocumentRDD(tableName: String): MapRDBBaseRDD[OJAIDocument] = {
    MapRSpark.builder
      .sparkSession(sparkSession.get)
      .sparkContext(sc.get)
      .configuration()
      .setDBCond(cond.orNull)
      .setColumnProjection(columns)
      .setQueryOptions(queryOptions.get)
      .setTable(tableName)
      .setBufferWrites(bufferWrites.get)
      .build()
      .toRDD[OJAIDocument]()
  }

  def toDF[T <: Product: TypeTag](): DataFrame = {
    toDF(null, GenerateSchema.SAMPLE_SIZE, true)
  }

  def toDF[T <: Product: TypeTag](Schema: StructType): DataFrame = {
    toDF(Schema, GenerateSchema.SAMPLE_SIZE, true)
  }

  def toDF[T <: Product: TypeTag](Schema: StructType, bufferWrites: Boolean): DataFrame = {
    toDF(Schema, GenerateSchema.SAMPLE_SIZE, bufferWrites)
  }

  def toDF[T <: Product: TypeTag](Schema: StructType,
                                  sampleSize: Double,
                                  bufferWrites: Boolean): DataFrame = {
    var derived: StructType = null
    if (Schema != null) derived = Schema
    else {
      derived = GenerateSchema.reflectSchema[T] match {
        case Some(reflectedSchema) => reflectedSchema
        case None =>
          GenerateSchema(toOJAIDocumentRDD(tableName.get), sampleSize)
      }
    }
    toDataFrame(derived, sampleSize, bufferWrites)
  }
}
