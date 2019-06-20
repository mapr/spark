package com.mapr.db.spark.sql.v2

import java.util

import com.mapr.db.spark.utils.LoggingTrait
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

abstract class MapRDBDataSourceReader(schema: StructType, tablePath: String, hintedIndexes: List[String])
  extends DataSourceReader
    with LoggingTrait
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  import collection.JavaConversions._

  private var supportedFilters: List[Filter] = List.empty

  private var projections: Option[StructType] = None

  override def readSchema(): StructType = projections match {
    case None => schema
    case Some(fieldsToProject) => fieldsToProject
  }

  /**
    * Creates a reader for each MapR-DB tablet.
    * Each of the reader has enough information to read the MapR-DB tablet, that is query range, physical location, etc.
    *
    * @return A list of Readers that Spark uses. Each reader will be launched within a Spark task.
    */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] =
    com.mapr.db.MapRDB
      .getTable(tablePath)
      .getTabletInfos
      .zipWithIndex
      .map { case (descriptor, idx) => MapRDBTabletInfo(idx, descriptor.getLocations, descriptor.getCondition.asJsonString) }
      .map(createReaderFactory)
      .toList

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition(isSupportedFilter)
    supportedFilters = supported.toList

    unsupported
  }

  override def pushedFilters(): Array[Filter] = supportedFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = projections = Some(requiredSchema)

  protected def createReaderFactory(tabletInfo: MapRDBTabletInfo): MapRDBDataPartitionReader = {
    logTabletInfo(tabletInfo)

    new MapRDBDataPartitionReader(
      tablePath,
      supportedFilters,
      readSchema(),
      tabletInfo,
      hintedIndexes)
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

  private def logTabletInfo(tabletInfo: MapRDBTabletInfo) =
    logDebug(
      s"TABLET: ${tabletInfo.internalId} ; " +
        s"PREFERRED LOCATIONS: ${tabletInfo.locations.mkString("[", ",", "]")} ; " +
        s"QUERY: ${tabletInfo.queryJson}")
}