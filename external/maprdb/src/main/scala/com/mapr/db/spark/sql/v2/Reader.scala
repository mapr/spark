package com.mapr.db.spark.sql.v2

import com.mapr.db.spark.utils.LoggingTrait
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.util.Try

/**
  * Entry point to the DataSource Reader
  */
class Reader extends ReadSupport with DataSourceRegister with LoggingTrait {

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {

    val tablePath = options.get("path").get()

    logInfo(s"TABLE PATH BEING USED: $tablePath")

    val hintedIndexes = options.get("idx").orElse("").trim.split(",").toList

    val readersPerTablet = getNumberOfReaders(options)

    new MapRDBDataSourceMultiReader(schema, tablePath, hintedIndexes, readersPerTablet)
  }

  private def getNumberOfReaders(options: DataSourceOptions): Int = Try {
    val numberOfReaders = options.get("readers").orElse("1").toInt

    if (numberOfReaders < 1) 1 else numberOfReaders

  }.getOrElse(1)

  /**
    * Creates a MapRDBDataSourceReader. Since no schema has been provided, we used the most generic possible schema
    * [_id].
    *
    * @param options the options for the returned data source reader, which is an immutable
    * @return
    */
  override def createReader(options: DataSourceOptions): DataSourceReader =
    createReader(new StructType().add("_id", StringType), options)

  override def shortName(): String = "MapRDB"
}