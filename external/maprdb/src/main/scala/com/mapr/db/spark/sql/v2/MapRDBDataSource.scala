package com.mapr.db.spark.sql.v2

import com.mapr.db.spark.utils.LoggingTrait
import java.util

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MapRDBDataSource
    extends DataSourceRegister
      with TableProvider
      with LoggingTrait {
  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = "MapRDB"

  /**
   * Infer the schema of the table identified by the given options.
   *
   * @param options an immutable case-insensitive string-to-string map that can identify a table,
   *                e.g. file path, Kafka topic name, etc.
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // TODO STUB. Implement me
    null
  }

  /**
   * Return a {@link Table} instance with the specified table schema, partitioning and properties
   * to do read/write. The returned table should report the same schema and partitioning with the
   * specified ones, or Spark may fail the operation.
   *
   * @param schema       The specified table schema.
   * @param partitioning The specified table partitioning.
   * @param properties   The specified table properties. It's case preserving (contains exactly what
   *                     users specified) and implementations are free to use it case sensitively or
   *                     insensitively. It should be able to identify a table, e.g. file path, Kafka
   *                     topic name, etc.
   */
  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]
                       ): Table = {

    val props = new CaseInsensitiveStringMap(properties)

    val tablePath = props.getOrDefault("path", "")
    require(tablePath.nonEmpty, "Path to MapR-DB table is required")
    logInfo(s"TABLE PATH BEING USED: $tablePath")

    val hintedIndexes = props.getOrDefault("idx", "")
      .trim
      .split(",")

    val readersPerTablet = props.getInt("readers", 1)

    MapRDBTable(schema, tablePath, hintedIndexes, readersPerTablet)
  }

//  private def getNumberOfReaders(props: CaseInsensitiveStringMap): Int = Try {
//    val numberOfReaders = props.getOrDefault("readers", "1").toInt
//    if (numberOfReaders < 1) 1 else numberOfReaders
//  }.getOrElse(1)
}
