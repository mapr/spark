package com.mapr.db.spark.sql.v2

import scala.collection.JavaConverters._

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class MapRDBTable(
      schema: StructType,
      tablePath: String,
      hintedIndexes: Array[String],
      readersPerTablet: Int)
    extends Table
      with SupportsRead {

  /**
   * A name to identify this table. Implementations should provide a meaningful name, like the
   * database and table name from catalog, or the location of files for this table.
   */
  override def name(): String = tablePath

  /**
   * Returns the set of capabilities for this table.
   */
  override def capabilities(): util.Set[TableCapability] =
    Set(BATCH_READ, BATCH_WRITE, TRUNCATE).asJava

  /**
   * Returns a {@link ScanBuilder} which can be used to build a {@link Scan}. Spark will call this
   * method to configure each data source scan.
   *
   * @param options The options for reading, which is an immutable case-insensitive
   *                string-to-string map.
   */
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    MapRDBScanBuilder(schema, tablePath, hintedIndexes, readersPerTablet)
}
