/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.utils

import com.mapr.db.MapRDB
import com.mapr.db.spark.RDD.{MapRDBTableScanRDD, RDDTYPE}
import com.mapr.db.spark.condition.{DBQueryCondition, Predicate}
import com.mapr.db.spark.configuration.SerializableConfiguration

import scala.reflect._
import scala.reflect.runtime.universe._
import org.apache.spark.SparkContext
import com.mapr.db.impl.ConditionImpl
import org.apache.hadoop.conf.Configuration
import org.ojai.store.{DocumentMutation, QueryCondition}
import com.mapr.db.spark.utils.DefaultClass.DefaultType
import com.mapr.db.spark.impl.OJAIDocument
import org.apache.spark.sql._
import com.mapr.db.spark.RDD.MapRDBBaseRDD
import com.mapr.db.spark._
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.sql.GenerateSchema
import com.mapr.db.spark.sql.utils.MapRSqlUtils
import org.apache.spark.sql.types.StructType

object MapRSpark {

  val defaultSource = "com.mapr.db.spark.sql.DefaultSource"

  def builder(): Builder = new Builder

  def save[D](dataset: Dataset[D], tableName: String, idFieldPath:String, createTable: Boolean, bulkInsert:Boolean): Unit = {
    val documentRdd = dataset.toDF.rdd.map(MapRSqlUtils.rowToDocument(_))
    documentRdd.saveToMapRDB(tableName, createTable = createTable, bulkInsert = bulkInsert, idFieldPath = idFieldPath)
  }

  def insert[D](dataset: Dataset[D], tableName: String, idFieldPath:String, createTable: Boolean, bulkInsert:Boolean): Unit = {
    val documentRdd = dataset.toDF.rdd.map(MapRSqlUtils.rowToDocument(_))
    documentRdd.insertToMapRDB(tableName, createTable = createTable, bulkInsert = bulkInsert, idFieldPath = idFieldPath)
  }

  def update[D](dataset: Dataset[D], tableName: String, idFieldPath:String, createTable: Boolean, bulkInsert:Boolean): Unit = {
    val documentRdd = dataset.toDF.rdd.map(MapRSqlUtils.rowToDocument(_))
    documentRdd.saveToMapRDB(tableName, createTable = createTable, bulkInsert = bulkInsert, idFieldPath = idFieldPath)
  }

  def update(df: DataFrame, tableName: String, mutation: (Row) => DocumentMutation,getID: (Row) => org.ojai.Value): Unit = {
    val documentRdd = df.rdd
    documentRdd.updateToMapRDB(tableName, mutation, getID)
  }

  def update(df: DataFrame, tableName: String, mutation: (Row) => DocumentMutation,getID: (Row) => org.ojai.Value, condition: Predicate): Unit = {
    val documentRdd = df.rdd
    documentRdd.updateToMapRDB(tableName, mutation, getID, condition)
  }

  def save(
           dfw: DataFrameWriter[_],
           tableName: String,
           idFieldPath: String,
           bulkInsert: Boolean
          ): Unit = {
    dfw.format(defaultSource)
      .option("tableName", tableName)
      .option("idFieldPath", idFieldPath)
      .option("bulkMode", bulkInsert)
      .save()
  }

  def load(sc: SparkContext, tableName: String) : MapRDBBaseRDD[OJAIDocument] = {
    sc.loadFromMapRDB(tableName)
  }

  class Builder {
    private var sparkctx : Option[SparkContext] = None
    private var sparkSession : Option[SparkSession] = None
    private var condition: Option[QueryCondition] = None
    private var dbcondition: Option[DBQueryCondition] = None
    private var tableName: Option[String] = None
    private var sampleSize: Option[String] = None
    private var conf: Option[SerializableConfiguration] = None
    private var beanClass: Option[Class[_]] = None
    private var columnProjection: Option[Seq[String]] = None

    def build(): MapRSpark = {
      require(sparkctx.isDefined, "The SparkContext must be set.")
      require(tableName.isDefined, "Source should be set")
      require(conf.isDefined, "Configuration should be set")
      val cond = condition match {
        case Some(x) => Option(x)
        case None => Option(DBClient().newCondition().build)
      }

      new MapRSpark(sparkSession, sparkctx, conf, dbcondition, tableName, columnProjection)
    }

    def configuration(conf: Configuration = new Configuration): Builder = {
      val sercnf = new SerializableConfiguration(conf)
      this.conf = Option(sercnf)
      this
    }

    def sparkContext(ctx: SparkContext): Builder = {
      this.sparkctx = Option(ctx)
      this.sparkSession = Option(SparkSession.builder().config(ctx.getConf).getOrCreate())
      this
    }

    def sparkSession(sparkSession: SparkSession): Builder = {
      this.sparkSession = Option(sparkSession)
      this.sparkctx = Option(sparkSession.sparkContext)
      this
    }

    def setCond(cond: Option[QueryCondition]): Builder = {
      this.condition = cond
      this.dbcondition = if(cond.isDefined) Option(DBQueryCondition(cond.get)) else None
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

    def setBeanClass(beanClass: Class[_]): Builder = {
      this.beanClass = Option(beanClass)
      this
    }

    def setColumnProjection(columns: Option[Seq[String]]) : Builder = {
      this.columnProjection = columns
      this
    }
  }
}

case class MapRSpark(sparkSession: Option[SparkSession], sc: Option[SparkContext], conf: Option[SerializableConfiguration], cond: Option[DBQueryCondition],
                     tableName: Option[String], columns: Option[Seq[String]]) {

  def toRDD[T : ClassTag](beanClass: Class[T] = null)(implicit e: T DefaultType OJAIDocument, f: RDDTYPE[T]): MapRDBTableScanRDD[T] = rdd[T](beanClass)

  def toJavaRDD[D: ClassTag](clazz: Class[D])(implicit f : RDDTYPE[D]) = toRDD[D](clazz)

  private def rdd[T : ClassTag](beanClass: Class[T])(implicit e: T DefaultType OJAIDocument, f: RDDTYPE[T]) : MapRDBTableScanRDD[T] =
    new MapRDBTableScanRDD[T](sparkSession.get, sc.get, sc.get.broadcast(conf.get), columns.getOrElse(null), tableName.get, cond.getOrElse(null), beanClass)


  def toDataFrame(schema: StructType, sampleSize: Double): DataFrame = {
    var reader: DataFrameReader =  sparkSession.get.read.format("com.mapr.db.spark.sql")
      .schema(schema).option("tableName", this.tableName.get)
      .option("sampleSize", sampleSize)

    if (cond.isDefined) reader = reader.option("QueryCondition",
                                              new String(cond.get.condition.asInstanceOf[ConditionImpl].getDescriptor.getSerialized.array))

    if (columns.isDefined) reader = reader.option("ColumnProjection", columns.getOrElse(Seq("")).reduce[String]((str1,str2) => str1+","+str2))
    reader.load()
  }

  def toOJAIDocumentRDD(tableName: String) : MapRDBBaseRDD[OJAIDocument] = {
    MapRSpark.builder.sparkSession(sparkSession.get).sparkContext(sc.get)
             .configuration().setDBCond(cond.getOrElse(null)).setColumnProjection(columns)
             .setTable(tableName).build().toRDD[OJAIDocument]()
  }

  def toDF[T <: Product : TypeTag]() : DataFrame = {
    toDF(null, GenerateSchema.SAMPLE_SIZE)
  }

  def toDF[T <: Product : TypeTag](Schema: StructType) : DataFrame = {
    toDF(Schema, GenerateSchema.SAMPLE_SIZE)
  }

  def toDF[T <: Product : TypeTag](Schema: StructType, sampleSize: Double): DataFrame = {
    var derived : StructType = null
    if (Schema != null) derived = Schema
    else {
      derived = GenerateSchema.reflectSchema[T] match {
        case Some(reflectedSchema) => reflectedSchema
        case None => GenerateSchema(toOJAIDocumentRDD(tableName.get), sampleSize)
      }
    }
    toDataFrame(derived, sampleSize)
  }
}
