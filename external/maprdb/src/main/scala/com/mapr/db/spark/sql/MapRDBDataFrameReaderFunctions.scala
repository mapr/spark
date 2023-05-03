/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import scala.reflect.runtime.universe._

import com.mapr.db.spark.utils.LoggingTrait

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.apache.spark.sql.types.StructType

private[spark] case class MapRDBDataFrameReaderFunctions(
    @transient dfr: DataFrameReader)
    extends LoggingTrait {

  def maprdb(): DataFrame = createDataFrame(None, None)

  /**
    * Creates a [[DataFrame]] through schema inference via the `T` type,
    * otherwise will sample the collection to
    * determine the type.
    *
    * @tparam T The optional type of the data from MapRDB
    * @return DataFrame
    */
  def maprdb[T <: Product: TypeTag](): DataFrame =
    createDataFrame(GenerateSchema.reflectSchema[T](), None)

  def maprdb(tableName: String): DataFrame =
    createDataFrame(None, Option(Map("tablePath" -> tableName)))

  /**
    * Creates a [[DataFrame]] through schema inference via the `T` type,
    * otherwise will sample the collection to
    * determine the type.
    *
    * @param configuration any connection read configuration overrides.
    *                      Overrides the configuration set in [[org.apache.spark.SparkConf]]
    * @tparam T The optional type of the data from MapRDB
    * @return DataFrame
    */
  def maprdb[T <: Product: TypeTag](
      configuration: Map[String, String]): DataFrame =
    createDataFrame(GenerateSchema.reflectSchema[T](), Some(configuration))

  /**
    * Creates a [[DataFrame]] with the set schema
    *
    * @param schema the schema definition
    * @return DataFrame
    */
  def maprdb(schema: StructType): DataFrame =
    createDataFrame(Some(schema), None)

  /**
    * Creates a [[DataFrame]] with the set schema
    *
    * @param schema the schema definition
    * @param configuration any custom read configuration
    * @return DataFrame
    */
  def maprdb(schema: StructType,
             configuration: Map[String, String]): DataFrame =
    createDataFrame(Some(schema), Some(configuration))

  private def createDataFrame(
      schema: Option[StructType],
      readConfig: Option[Map[String, String]]): DataFrame = {
    val builder = dfr.format("com.mapr.db.spark.sql.DefaultSource")
    if (schema.isDefined) builder.schema(schema.get)
    if (readConfig.isDefined) builder.options(readConfig.get)
    builder.load()
  }
}
