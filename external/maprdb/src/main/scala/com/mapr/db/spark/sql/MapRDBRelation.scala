/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import com.mapr.db.spark.RDD.MapRDBBaseRDD
import com.mapr.db.spark.condition.Predicate
import com.mapr.db.spark.field
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.LoggingTrait
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.IsNotNull
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.sources._
import com.mapr.db.spark.sql.utils._
import org.ojai.store.QueryCondition
import org.ojai.types.{ODate, OTimestamp}

import scala.collection.mutable.ArrayBuffer

private[spark]
case class MapRDBRelation(tableName: String, relationSchema: StructType,
                          rdd: MapRDBBaseRDD[OJAIDocument], Operation: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation with LoggingTrait {

  lazy val schema = relationSchema

  override def buildScan(requiredColumns: Array[String],
                         filters: Array[Filter]) : RDD[Row] = {

    var optimizedRdd = rdd
    val queryConditions = schema.fields.filter(!_.nullable)
                                .map(_.name).map(IsNotNull) ++ filters

    if (requiredColumns.nonEmpty || queryConditions.nonEmpty) {
      logDebug(s"requiredColumns: ${requiredColumns.mkString(",")}, filters: ${queryConditions.mkString(",")}")
    }

    val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
    for (elem <- requiredColumns) { fields += schema.fields(schema.fieldIndex(elem))}

    optimizedRdd = if (!requiredColumns.isEmpty)
                      optimizedRdd.select(requiredColumns:_*) else rdd

    optimizedRdd = if (!queryConditions.isEmpty)
                      optimizedRdd.where(convertToCondition(filters, false)) else optimizedRdd

    optimizedRdd.map(doc => doc.asReader()).mapPartitions(MapRSqlUtils.documentsToRow(_, StructType(fields), requiredColumns))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    val dfw = data.write.format("com.mapr.db.spark.sql")
                        .option("tableName", tableName).option("sampleSize", 1000)

    overwrite match {
      case true => dfw.mode(SaveMode.Overwrite).save()
      case false => dfw.mode(SaveMode.Append).save()
    }
  }

  private def getPredicate(fieldName: String, operation: QueryCondition.Op , value: Any): Predicate = {
    operation match {
      case QueryCondition.Op.EQUAL => EqualsTo(fieldName, value)
      case QueryCondition.Op.GREATER => Greater(fieldName, value)
      case QueryCondition.Op.GREATER_OR_EQUAL => GreaterEqualsTo(fieldName, value)
      case QueryCondition.Op.LESS => Lesser(fieldName, value)
      case QueryCondition.Op.LESS_OR_EQUAL => LessEqualsTo(fieldName, value)
      case QueryCondition.Op.NOT_EQUAL => NotEqualsTo(fieldName, value)
      case _ => throw new RuntimeException(s"QueryCondition operation $operation not supported")
    }
  }

  private def convertToODate(dt: java.sql.Date): ODate = {
    new ODate(dt.getDay)
  }

  private def convertToOTimeStamp(timeStamp: java.sql.Timestamp): OTimestamp = {
    new OTimestamp(timeStamp.getTime)
  }

  private def EqualsTo(fieldName: String, value: Any): Predicate = {
    if (value.isInstanceOf[Int]) field(fieldName) === value.asInstanceOf[Int]
    else if(value.isInstanceOf[Byte]) field(fieldName) === value.asInstanceOf[Byte]
    else if(value.isInstanceOf[Short]) field(fieldName) === value.asInstanceOf[Short]
    else if(value.isInstanceOf[Long]) field(fieldName) === value.asInstanceOf[Long]
    else if(value.isInstanceOf[Float]) field(fieldName) === value.asInstanceOf[Float]
    else if(value.isInstanceOf[Double]) field(fieldName) === value.asInstanceOf[Double]
    else if(value.isInstanceOf[String]) field(fieldName) === value.asInstanceOf[String]
    else if(value.isInstanceOf[BigDecimal]) field(fieldName) === value.asInstanceOf[BigDecimal]
    else if(value.isInstanceOf[java.sql.Date]) field(fieldName) === convertToODate(value.asInstanceOf[java.sql.Date])
    else if(value.isInstanceOf[java.sql.Timestamp]) field(fieldName) === convertToOTimeStamp(value.asInstanceOf[java.sql.Timestamp])
    else throw new RuntimeException(s"Cannot convert $value to a MapRDB predicate")
  }

  private def NotEqualsTo(fieldName: String, value: Any): Predicate = {
    if (value.isInstanceOf[Int]) field(fieldName).!=(value.asInstanceOf[Int])
    else if(value.isInstanceOf[Byte]) field(fieldName).!=(value.asInstanceOf[Byte])
    else if(value.isInstanceOf[Short]) field(fieldName).!=(value.asInstanceOf[Short])
    else if(value.isInstanceOf[Long]) field(fieldName).!=(value.asInstanceOf[Long])
    else if(value.isInstanceOf[Float]) field(fieldName).!=(value.asInstanceOf[Float])
    else if(value.isInstanceOf[Double]) field(fieldName).!=(value.asInstanceOf[Double])
    else if(value.isInstanceOf[String]) field(fieldName).!=(value.asInstanceOf[String])
    else if(value.isInstanceOf[BigDecimal]) field(fieldName).!=(value.asInstanceOf[BigDecimal])
    else if(value.isInstanceOf[java.sql.Date]) field(fieldName).!=(convertToODate(value.asInstanceOf[java.sql.Date]))
    else if(value.isInstanceOf[java.sql.Timestamp]) field(fieldName).!=(convertToOTimeStamp(value.asInstanceOf[java.sql.Timestamp]))
    else if(value.isInstanceOf[String]) field(fieldName).!=(value.asInstanceOf[String])
    else throw new RuntimeException(s"Cannot convert $value to a MapRDB predicate")
  }

  private def Lesser(fieldName: String, value: Any): Predicate = {
    if (value.isInstanceOf[Int]) field(fieldName) < value.asInstanceOf[Int]
    else if(value.isInstanceOf[Byte]) field(fieldName) < value.asInstanceOf[Byte]
    else if(value.isInstanceOf[Short]) field(fieldName) < value.asInstanceOf[Short]
    else if(value.isInstanceOf[Long]) field(fieldName) < value.asInstanceOf[Long]
    else if(value.isInstanceOf[Float]) field(fieldName) < value.asInstanceOf[Float]
    else if(value.isInstanceOf[Double]) field(fieldName) < value.asInstanceOf[Double]
    else if(value.isInstanceOf[BigDecimal]) field(fieldName) < value.asInstanceOf[BigDecimal]
    else if(value.isInstanceOf[java.sql.Date]) field(fieldName) < convertToODate(value.asInstanceOf[java.sql.Date])
    else if(value.isInstanceOf[java.sql.Timestamp]) field(fieldName) < convertToOTimeStamp(value.asInstanceOf[java.sql.Timestamp])
    else if(value.isInstanceOf[String]) field(fieldName) < value.asInstanceOf[String]
    else throw new RuntimeException(s"Cannot convert $value to a MapRDB predicate")
  }

  private def LessEqualsTo(fieldName: String, value: Any): Predicate = {
    if (value.isInstanceOf[Int]) field(fieldName) <= value.asInstanceOf[Int]
    else if(value.isInstanceOf[Byte]) field(fieldName) <= value.asInstanceOf[Byte]
    else if(value.isInstanceOf[Short]) field(fieldName) <= value.asInstanceOf[Short]
    else if(value.isInstanceOf[Long]) field(fieldName) <= value.asInstanceOf[Long]
    else if(value.isInstanceOf[Float]) field(fieldName) <= value.asInstanceOf[Float]
    else if(value.isInstanceOf[Double]) field(fieldName) <= value.asInstanceOf[Double]
    else if(value.isInstanceOf[BigDecimal]) field(fieldName) <= value.asInstanceOf[BigDecimal]
    else if(value.isInstanceOf[java.sql.Date]) field(fieldName) <= convertToODate(value.asInstanceOf[java.sql.Date])
    else if(value.isInstanceOf[java.sql.Timestamp]) field(fieldName) <= convertToOTimeStamp(value.asInstanceOf[java.sql.Timestamp])
    else if(value.isInstanceOf[String]) field(fieldName) <= value.asInstanceOf[String]
    else throw new RuntimeException(s"Cannot convert $value to a MapRDB predicate")
  }

  private def Greater(fieldName: String, value: Any): Predicate = {
    if (value.isInstanceOf[Int]) field(fieldName) > value.asInstanceOf[Int]
    else if(value.isInstanceOf[Byte]) field(fieldName) > value.asInstanceOf[Byte]
    else if(value.isInstanceOf[Short]) field(fieldName) > value.asInstanceOf[Short]
    else if(value.isInstanceOf[Long]) field(fieldName) > value.asInstanceOf[Long]
    else if(value.isInstanceOf[Float]) field(fieldName) > value.asInstanceOf[Float]
    else if(value.isInstanceOf[Double]) field(fieldName) > value.asInstanceOf[Double]
    else if(value.isInstanceOf[BigDecimal]) field(fieldName) > value.asInstanceOf[BigDecimal]
    else if(value.isInstanceOf[java.sql.Date]) field(fieldName) > convertToODate(value.asInstanceOf[java.sql.Date])
    else if(value.isInstanceOf[java.sql.Timestamp]) field(fieldName) > convertToOTimeStamp(value.asInstanceOf[java.sql.Timestamp])
    else if(value.isInstanceOf[String]) field(fieldName) > value.asInstanceOf[String]
    else throw new RuntimeException(s"Cannot convert $value to a MapRDB predicate")
  }

  private def GreaterEqualsTo(fieldName: String, value: Any): Predicate = {
    if (value.isInstanceOf[Int]) field(fieldName) >= value.asInstanceOf[Int]
    else if(value.isInstanceOf[Byte]) field(fieldName) >= value.asInstanceOf[Byte]
    else if(value.isInstanceOf[Short]) field(fieldName) >= value.asInstanceOf[Short]
    else if(value.isInstanceOf[Long]) field(fieldName) >= value.asInstanceOf[Long]
    else if(value.isInstanceOf[Float]) field(fieldName) >= value.asInstanceOf[Float]
    else if(value.isInstanceOf[Double]) field(fieldName) >= value.asInstanceOf[Double]
    else if(value.isInstanceOf[BigDecimal]) field(fieldName) >= value.asInstanceOf[BigDecimal]
    else if(value.isInstanceOf[java.sql.Date]) field(fieldName) >= convertToODate(value.asInstanceOf[java.sql.Date])
    else if(value.isInstanceOf[java.sql.Timestamp]) field(fieldName) >= convertToOTimeStamp(value.asInstanceOf[java.sql.Timestamp])
    else if(value.isInstanceOf[String]) field(fieldName) >= value.asInstanceOf[String]
    else throw new RuntimeException(s"Cannot convert $value to a MapRDB predicate")
  }

  private def convertToCondition(filters: Array[Filter], inNot: Boolean): Predicate = {
    val resultPredicate : Array[Predicate] = filters.map {
        case EqualTo(fld, value)              =>    {
          if (!inNot)
            getPredicate(fld, QueryCondition.Op.EQUAL, value)
          else getPredicate(fld, QueryCondition.Op.NOT_EQUAL, value)
        }
        case EqualNullSafe(fld, value)        =>    {
          if (!inNot)
             getPredicate(fld, QueryCondition.Op.EQUAL, value)
          else getPredicate(fld, QueryCondition.Op.NOT_EQUAL, value)
        }
        case GreaterThan(fld, value)          =>    {
          if (!inNot)
             getPredicate(fld, QueryCondition.Op.GREATER, value)
          else  getPredicate(fld, QueryCondition.Op.LESS_OR_EQUAL, value)
        }
        case GreaterThanOrEqual(fld, value)   =>    {
          if (!inNot)
             getPredicate(fld, QueryCondition.Op.GREATER_OR_EQUAL, value)
          else  getPredicate(fld, QueryCondition.Op.LESS, value)
        }
        case In(fld, values)                  =>    {
          if (!inNot)
             field(fld) in  values
          else field(fld) notin values
        }
        case LessThan(fld, value)             =>    {
          if (!inNot)
             getPredicate(fld, QueryCondition.Op.LESS, value)
          else getPredicate(fld, QueryCondition.Op.GREATER_OR_EQUAL, value)
        }
        case LessThanOrEqual(fld, value)      =>    {
          if (!inNot)
             getPredicate(fld, QueryCondition.Op.LESS_OR_EQUAL, value)
          else  getPredicate(fld, QueryCondition.Op.GREATER, value)
        }
        case IsNull(fld)                      =>    {
          if (!inNot)
             field(fld) notexists
          else  field(fld) exists
        }
        case IsNotNull(fld)                   =>    {
          if (!inNot)
             field(fld) exists
          else  field(fld) notexists
        }
        case And(leftFilter, rightFilter)     =>    {
          if (!inNot)
             convertToCondition(Array(leftFilter), inNot) and convertToCondition(Array(rightFilter), inNot)
          else convertToCondition(Array(leftFilter), inNot) or convertToCondition(Array(rightFilter), inNot)
        }
        case Or(leftFilter, rightFilter)      =>    {
          if (!inNot)
            convertToCondition(Array(leftFilter), inNot) or  convertToCondition(Array(rightFilter), inNot)
          else convertToCondition(Array(leftFilter), inNot) and convertToCondition(Array(rightFilter), inNot)
        }
        case Not(filter)                      =>    {
          if (!inNot)
            convertToCondition(Array(filter), true)
          else convertToCondition(Array(filter), false)

        }
        case StringStartsWith(fld, value)     =>    {
          if (!inNot)
            field(fld)  like  s"$value%"
          else field(fld)  notlike s"$value%"
        }
        case StringEndsWith(fld, value)       =>    {
          if (!inNot)
            field(fld)  like  s"%$value"
          else field(fld)  notlike s"%$value"
        }
        case StringContains(fld, value)       =>    {
          if (!inNot)
            field(fld)  like  s"%$value%"
          else field(fld)  notlike s"$value"
        }
        case _                                =>    null
      }
    resultPredicate.filter(_ != null).reduceLeft[Predicate]((predicate1, predicate2) => predicate1 and predicate2)
  }
}
