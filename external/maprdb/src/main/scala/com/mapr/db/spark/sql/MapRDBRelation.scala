/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import scala.collection.mutable.ArrayBuffer

import com.mapr.db.spark.RDD.MapRDBBaseRDD
import com.mapr.db.spark.condition.Predicate
import com.mapr.db.spark.field
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.sql.utils._
import com.mapr.db.spark.utils.LoggingTrait
import org.ojai.store.QueryCondition
import org.ojai.types.{ODate, OTimestamp}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

private[spark] case class MapRDBRelation(
                                          tableName: String,
                                          relationSchema: StructType,
                                          rdd: MapRDBBaseRDD[OJAIDocument],
                                          Operation: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with LoggingTrait {

  lazy val schema = relationSchema

  override def buildScan(requiredColumns: Array[String],
                         filters: Array[Filter]): RDD[Row] = {

    val queryConditions = schema.fields
      .filter(!_.nullable)
      .map(_.name)
      .map(IsNotNull) ++ filters

    if (requiredColumns.nonEmpty || queryConditions.nonEmpty) {
      logDebug(s"requiredColumns: ${requiredColumns.mkString(",")
      }, filters: ${queryConditions.mkString(",")}")
    }

    val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
    for (elem <- requiredColumns) {
      fields += schema.fields(schema.fieldIndex(elem))
    }

    val optimizedByColumns =
      if (!requiredColumns.isEmpty) {
        rdd.select(requiredColumns: _*)
      } else rdd

    val optimizedByFilter =
      if (!queryConditions.isEmpty) {
        optimizedByColumns.where(convertToCondition(queryConditions, false))
      } else optimizedByColumns

    optimizedByFilter
      .map(doc => doc.asReader())
      .mapPartitions(
        MapRSqlUtils.documentsToRow(_, StructType(fields), requiredColumns))
  }


  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    val dfw = data.write
      .format("com.mapr.db.spark.sql")
      .option("tablePath", tableName)
      .option("sampleSize", 1000)

    if (overwrite) {
      dfw.mode(SaveMode.Overwrite).save()
    } else {
      dfw.mode(SaveMode.Append).save()
    }
  }

  private def getPredicate(fieldName: String,
                           operation: QueryCondition.Op,
                           value: Any): Predicate = {

    import com.mapr.db.spark.condition._

    def getPredicate[T](typedValue: T)(implicit ev: quotes[T]): Predicate = operation match {
      case QueryCondition.Op.EQUAL => field(fieldName).===(typedValue)(ev)
      case QueryCondition.Op.NOT_EQUAL => field(fieldName).!=(typedValue)(ev)
      case QueryCondition.Op.GREATER => field(fieldName).>(typedValue)(ev)
      case QueryCondition.Op.GREATER_OR_EQUAL => field(fieldName).>=(typedValue)(ev)
      case QueryCondition.Op.LESS => field(fieldName).<(typedValue)(ev)
      case QueryCondition.Op.LESS_OR_EQUAL => field(fieldName).<=(typedValue)(ev)
      case _ =>
        throw new RuntimeException(
          s"QueryCondition operation $operation not supported")
    }

    value match {
      case i: Int => getPredicate(i)
      case b: Byte => getPredicate(b)
      case sh: Short => getPredicate(sh)
      case l: Long => getPredicate(l)
      case fl: Float => getPredicate(fl)
      case d: Double => getPredicate(d)
      case str: String => getPredicate(str)
      case decimal: BigDecimal => getPredicate(decimal)
      case date: java.sql.Date => getPredicate(convertToODate(date))
      case timestamp: java.sql.Timestamp => getPredicate(convertToOTimeStamp(timestamp))
      case _ => throw new RuntimeException(s"Cannot convert $value to a MapRDB predicate")
    }
  }

  private def convertToODate(date: java.sql.Date): ODate = {
    new ODate(date)
  }

  private def convertToOTimeStamp(timeStamp: java.sql.Timestamp): OTimestamp = {
    new OTimestamp(timeStamp.getTime)
  }

  private def convertToCondition(filters: Array[Filter],
                                 inNot: Boolean): Predicate = {
    val resultPredicate: Array[Predicate] = filters.map {
      case EqualTo(fld, value) =>
        if (!inNot) getPredicate(fld, QueryCondition.Op.EQUAL, value)
        else getPredicate(fld, QueryCondition.Op.NOT_EQUAL, value)
      case EqualNullSafe(fld, value) =>
        if (!inNot) getPredicate(fld, QueryCondition.Op.EQUAL, value)
        else getPredicate(fld, QueryCondition.Op.NOT_EQUAL, value)
      case GreaterThan(fld, value) =>
        if (!inNot) getPredicate(fld, QueryCondition.Op.GREATER, value)
        else getPredicate(fld, QueryCondition.Op.LESS_OR_EQUAL, value)
      case GreaterThanOrEqual(fld, value) =>
        if (!inNot) getPredicate(fld, QueryCondition.Op.GREATER_OR_EQUAL, value)
        else getPredicate(fld, QueryCondition.Op.LESS, value)
      case In(fld, values) =>
        if (!inNot) field(fld) in values
        else field(fld) notin values
      case LessThan(fld, value) =>
        if (!inNot) getPredicate(fld, QueryCondition.Op.LESS, value)
        else getPredicate(fld, QueryCondition.Op.GREATER_OR_EQUAL, value)
      case LessThanOrEqual(fld, value) =>
        if (!inNot) getPredicate(fld, QueryCondition.Op.LESS_OR_EQUAL, value)
        else getPredicate(fld, QueryCondition.Op.GREATER, value)
      case IsNull(fld) =>
        if (!inNot) field(fld).typeof("NULL")
        else field(fld).nottypeof("NULL")
      case IsNotNull(fld) =>
        if (!inNot) field(fld).nottypeof("NULL")
        else field(fld).typeof("NULL")
      case And(leftFilter, rightFilter) =>
        if (!inNot) {
          convertToCondition(Array(leftFilter), inNot) and
            convertToCondition(Array(rightFilter), inNot)
        } else {
          convertToCondition(Array(leftFilter), inNot) or
            convertToCondition(Array(rightFilter), inNot)
        }
      case Or(leftFilter, rightFilter) =>
        if (!inNot) {
          convertToCondition(Array(leftFilter), inNot) or
            convertToCondition(Array(rightFilter), inNot)
        } else {
          convertToCondition(Array(leftFilter), inNot) and
            convertToCondition(Array(rightFilter), inNot)
        }
      case Not(filter) =>
        if (!inNot) convertToCondition(Array(filter), true)
        else convertToCondition(Array(filter), false)
      case StringStartsWith(fld, value) =>
        if (!inNot) field(fld) like s"$value%"
        else field(fld) notlike s"$value%"
      case StringEndsWith(fld, value) =>
        if (!inNot) field(fld) like s"%$value"
        else field(fld) notlike s"%$value"
      case StringContains(fld, value) =>
        if (!inNot) field(fld) like s"%$value%"
        else field(fld) notlike s"$value"
      case _ => null
    }

    resultPredicate
      .filter(_ != null)
      .reduceLeft[Predicate]((predicate1, predicate2) =>
      predicate1 and predicate2)
  }
}
