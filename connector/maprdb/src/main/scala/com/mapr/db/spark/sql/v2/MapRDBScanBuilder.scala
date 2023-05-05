package com.mapr.db.spark.sql.v2

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

case class MapRDBScanBuilder(
      schema: StructType,
      tablePath: String,
      hintedIndexes: Array[String],
      readersPerTablet: Int)
    extends ScanBuilder
      with SupportsPushDownFilters
      with SupportsPushDownRequiredColumns {

  private var supportedFilters = Array.empty[Filter]
  private var projections: StructType = schema

  override def build(): Scan = {
    MapRDBScan(projections, tablePath, hintedIndexes, supportedFilters, readersPerTablet)
  }

  /**
   * Pushes down filters, and returns filters that need to be evaluated after scanning.
   * <p>
   * Rows should be returned from the data source if and only if all of the filters match. That is,
   * filters must be interpreted as ANDed together.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition(isSupportedFilter)
    supportedFilters = supported

    unsupported
  }

  private def isSupportedFilter(filter: Filter): Boolean = filter match {
    case And(a, b) => isSupportedFilter(a) && isSupportedFilter(b)
    case Or(a, b) => isSupportedFilter(a) || isSupportedFilter(b)
    case _: IsNull => true
    case _: IsNotNull => true
    case _: In => true
    case _: StringStartsWith => true
    case EqualTo(_, value) => SupportedFilterTypes.isSupportedType(value)
    case LessThan(_, value) => SupportedFilterTypes.isSupportedType(value)
    case LessThanOrEqual(_, value) => SupportedFilterTypes.isSupportedType(value)
    case GreaterThan(_, value) => SupportedFilterTypes.isSupportedType(value)
    case GreaterThanOrEqual(_, value) => SupportedFilterTypes.isSupportedType(value)

    case _ => false
  }

  /**
   * Returns the filters that are pushed to the data source via {@link #pushFilters ( Filter [ ] )}.
   *
   * There are 3 kinds of filters:
   *  1. pushable filters which don't need to be evaluated again after scanning.
   *     2. pushable filters which still need to be evaluated after scanning, e.g. parquet
   *     row group filter.
   *     3. non-pushable filters.
   *     Both case 1 and 2 should be considered as pushed filters and should be returned by this method.
   *
   * It's possible that there is no filters in the query and {@link #pushFilters ( Filter [ ] )}
   * is never called, empty array should be returned for this case.
   */
  override def pushedFilters(): Array[Filter] =
    supportedFilters

  /**
   * Applies column pruning w.r.t. the given requiredSchema.
   *
   * Implementation should try its best to prune the unnecessary columns or nested fields, but it's
   * also OK to do the pruning partially, e.g., a data source may not be able to prune nested
   * fields, and only prune top-level columns.
   *
   * Note that, {@link Scan# readSchema ( )} implementation should take care of the column
   * pruning applied here.
   */
  override def pruneColumns(requiredSchema: StructType): Unit =
    projections = requiredSchema
}
