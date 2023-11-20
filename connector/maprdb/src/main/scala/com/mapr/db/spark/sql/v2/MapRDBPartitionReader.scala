package com.mapr.db.spark.sql.v2

import scala.collection.JavaConverters._

import com.mapr.db.spark.sql.utils.MapRSqlUtils
import com.mapr.db.spark.utils.LoggingTrait
import org.ojai.store._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Reads data from one particular MapR-DB tablet / region
 *
 * @param table         MapR-DB Table Path
 * @param filters       Filters to be pushed down
 * @param schema        Schema to be pushed down
 * @param hintedIndexes External index
 */
class MapRDBPartitionReader(table: String,
                            filters: Array[Filter],
                            schema: StructType,
                            partition: MapRDBInputPartition,
                            hintedIndexes: Array[String])
  extends PartitionReader[InternalRow] with LoggingTrait {

  private val toRow = {
    RowEncoder.encoderFor(schema).resolveAndBind().createSerializer()
  }

  logDebug(filters.mkString("FILTERS: [", ", ", "]"))
  logDebug(query.asJsonString())

  @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")
  @transient private lazy val store: DocumentStore = connection.getStore(table)
  @transient private lazy val documents = {
    val queryResult = store.find(query)
    logDebug(s"OJAI QUERY PLAN: ${queryResult.getQueryPlan}")

    queryResult.asScala.iterator
  }

  @transient private lazy val query: Query = {
    val finalQueryConditionString = if (filters.nonEmpty) {
      val sparkFiltersQueryCondition = QueryConditionBuilder.buildQueryConditionFrom(filters)(connection)

      QueryConditionBuilder.addTabletInfo(partition.queryJson, sparkFiltersQueryCondition)
    } else {
      partition.queryJson
    }

    logDebug(s"USING QUERY STRING: $finalQueryConditionString")
    logDebug(s"PROJECTIONS TO PUSH DOWN: $projectionsAsString")

    connection
      .newQuery()
      .where(finalQueryConditionString)
      .select(projectionsNames: _*)
      .setOptions(queryOptions)
      .build()
  }

  //  override def preferredLocations(): Array[String] = tabletInfo.locations

  override protected def logName: String =
    s"PARTITION_READER ===== TABLET: ${partition.internalId}"

  private def queryOptions =
    hintedIndexes.foldLeft(connection.newDocument())((doc, hint) =>
      doc.set("ojai.mapr.query.hint-using-index", hint))

  private def projectionsAsString: String =
    schema
      .fields
      .foldLeft(List.empty[(String, DataType)])((xs, field) => (field.name, field.dataType) :: xs)
      .mkString("[", ",", "]")

  private def projectionsNames: Array[String] = schema.fields.map(_.name)

  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  override def next(): Boolean =
    documents.hasNext

  /**
   * Return the current record. This method should return same value until `next` is called.
   */
  override def get(): InternalRow = {
    val document = documents.next()
    logDebug(document.asJsonString())
    val newDocument = com.mapr.db.spark.MapRDBSpark.newDocument(document)
    val row = MapRSqlUtils.documentToRow(newDocument, schema)

    toRow(row).copy()
  }

  override def close(): Unit = {
    store.close()
    connection.close()
  }
}
