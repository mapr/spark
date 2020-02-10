package com.mapr.db.spark.sql.v2

import com.mapr.db.spark.utils.LoggingTrait
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

/**
  * Reads data from one particular MapR-DB tablet / region
  *
  * @param table      MapR-DB Table Path
  * @param filters    Filters to be pushed down
  * @param schema     Schema to be pushed down
  * @param tabletInfo Specific information of the tablet being used by this reader
  */
class MapRDBDataPartitionReader(table: String,
                                filters: List[Filter],
                                schema: StructType,
                                tabletInfo: MapRDBTabletInfo,
                                hintedIndexes: List[String])
  extends InputPartition[InternalRow] with LoggingTrait {

  import com.mapr.db.spark.sql.utils.MapRSqlUtils._
  import org.ojai.store._

  import scala.collection.JavaConverters._

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

      QueryConditionBuilder.addTabletInfo(tabletInfo.queryJson, sparkFiltersQueryCondition)
    } else {
      tabletInfo.queryJson
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

  override def preferredLocations(): Array[String] = tabletInfo.locations

  override protected def logName: String = "PARTITION_READER" + s" ===== TABLET: ${tabletInfo.internalId}"

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new InputPartitionReader[InternalRow] {
    override def next(): Boolean = documents.hasNext

    override def get(): InternalRow = {

      val document = documents.next()

      logDebug(document.asJsonString())

      val row = documentToRow(com.mapr.db.spark.MapRDBSpark.newDocument(document), schema)

      RowEncoder(schema).toRow(row)
    }

    override def close(): Unit = {
      store.close()
      connection.close()
    }
  }

  private def queryOptions =
    hintedIndexes
      .foldLeft(connection.newDocument())((doc, hint) => doc.set("ojai.mapr.query.hint-using-index", hint))

  private def projectionsAsString: String =
    schema
      .fields
      .foldLeft(List.empty[(String, DataType)])((xs, field) => (field.name, field.dataType) :: xs)
      .mkString("[", ",", "]")

  private def projectionsNames: Array[String] = schema.fields.map(_.name)
}
