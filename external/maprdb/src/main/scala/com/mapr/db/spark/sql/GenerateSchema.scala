/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import java.util.Arrays.sort
import java.util.Comparator

import scala.Array._
import scala.reflect.runtime.universe._

import com.mapr.db.spark.RDD.MapRDBBaseRDD
import com.mapr.db.spark.exceptions.SchemaMappingException
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.MapRSpark
import org.apache.spark.SparkContext
import org.ojai.DocumentReader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType, _}


object GenerateSchema {

  val SAMPLE_SIZE = 1000.toDouble

  private val ByteDecimal = DecimalType(3, 0)
  private val ShortDecimal = DecimalType(5, 0)
  private val IntDecimal = DecimalType(10, 0)
  private val LongDecimal = DecimalType(20, 0)
  private val FloatDecimal = DecimalType(14, 7)
  private val DoubleDecimal = DecimalType(30, 15)

  private def DecimalTypeforType(dataType: DataType): DecimalType =
    dataType match {
      case ByteType => ByteDecimal
      case ShortType => ShortDecimal
      case IntegerType => IntDecimal
      case LongType => LongDecimal
      case FloatType => FloatDecimal
      case DoubleType => DoubleDecimal
    }

  def apply(sc: SparkContext,
            tableName: String,
            sampleSize: Double): StructType = {
    apply(MapRSpark.load(sc, tableName), sampleSize)
  }

  def apply(rdd: MapRDBBaseRDD[OJAIDocument],
            sampleSize: Double): StructType = {
    apply(rdd, sampleSize, false)
  }

  def apply(rdd: MapRDBBaseRDD[OJAIDocument],
            sampleSize: Double,
            failureOnConflict: Boolean): StructType = {

    val numOfPartitions = rdd.getNumPartitions
    val eachPartitionLimit =
      java.lang.Math.ceil(sampleSize / numOfPartitions).toInt
    val sampleData: RDD[DocumentReader] = rdd
      .mapPartitions(iter => iter.take(eachPartitionLimit))
      .map(doc => doc.asReader())

    val resultType = sampleData
      .map(reader => inferTypeForField(failureOnConflict)(reader.next, reader))
      .treeAggregate[DataType](StructType(Seq()))(
      compatibleType(failureOnConflict),
      compatibleType(failureOnConflict))

    canonicalizeType(resultType) match {
      case Some(st: StructType) => st
      case _ => StructType(Seq())
    }
  }

  private def canonicalizeType: DataType => Option[DataType] = {
    case arrayType@ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        arrayType.copy(canonicalType)
      }
    case StructType(fields) =>
      val canonicalFields = for {
        field <- fields
        if field.name.nonEmpty
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.nonEmpty) {
        Some(StructType(canonicalFields))
      } else {
        None
      }
    case other => Some(other)
  }

  private val structFieldComparator = new Comparator[StructField] {
    override def compare(o1: StructField, o2: StructField): Int = {
      o1.name.compare(o2.name)
    }
  }

  private def inferTypeForField(failureOnConflict: Boolean)(
    event: DocumentReader.EventType,
    reader: DocumentReader): DataType = {

    event match {
      case DocumentReader.EventType.NULL => DataTypes.NullType

      case DocumentReader.EventType.START_ARRAY =>
        var elementType = DataTypes.NullType
        var thisEvent = reader.next
        while (thisEvent != DocumentReader.EventType.END_ARRAY) {
          elementType = compatibleType(failureOnConflict)(
            elementType,
            inferTypeForField(failureOnConflict)(thisEvent, reader))
          thisEvent = reader.next
        }
        ArrayType(elementType)
      case DocumentReader.EventType.START_MAP =>
        val builder = Array.newBuilder[StructField]
        var thisEvent = reader.next
        while (thisEvent != DocumentReader.EventType.END_MAP) {
          builder += StructField(
            reader.getFieldName,
            inferTypeForField(failureOnConflict)(thisEvent, reader)
          )
          thisEvent = reader.next
        }
        val fields = builder.result()
        sort(fields, structFieldComparator)
        StructType(fields)
      case DocumentReader.EventType.BINARY => DataTypes.BinaryType
      case DocumentReader.EventType.BOOLEAN => DataTypes.BooleanType
      case DocumentReader.EventType.TIMESTAMP => DataTypes.TimestampType
      case DocumentReader.EventType.DOUBLE => DataTypes.DoubleType
      case DocumentReader.EventType.INT => DataTypes.IntegerType
      case DocumentReader.EventType.LONG => DataTypes.LongType
      case DocumentReader.EventType.STRING => DataTypes.StringType
      case DocumentReader.EventType.FLOAT => DataTypes.FloatType
      case DocumentReader.EventType.BYTE => DataTypes.ByteType
      case DocumentReader.EventType.SHORT => DataTypes.ShortType
      case DocumentReader.EventType.DECIMAL =>
        DecimalType(reader.getDecimalPrecision, reader.getDecimalScale)
      case DocumentReader.EventType.DATE => DataTypes.DateType
      case DocumentReader.EventType.TIME => DataTypes.TimestampType
      case DocumentReader.EventType.INTERVAL => DataTypes.CalendarIntervalType
      case _ =>
        throw new RuntimeException(
          s"Type ${Option(event).toString} cannot be inferred")
    }
  }

  private def compatibleType(failureOnConflict: Boolean)
                            (dt1: DataType, dt2: DataType): DataType = {
    TypeCoercion.findTightestCommonType(dt1, dt2).getOrElse {
      (dt1, dt2) match {
        case (st1@StructType(fields1), st2@StructType(fields2)) =>
          if (isInvalidType(st1)) return st1
          if (isInvalidType(st2)) return st2
          val newFields =
            (fields1 ++ fields2).groupBy(field => field.name).map {
              case (name, fieldTypes) =>
                try {
                  val dataType = fieldTypes.view
                    .map(_.dataType)
                    .reduce(compatibleType(failureOnConflict))
                  StructField(name, dataType)
                } catch {
                  case e: SchemaMappingException =>
                    throw new SchemaMappingException(
                      s"Schema cannot be inferred for the column $name")
                }
            }
          StructType(newFields.toSeq.sortBy(_.name))
        case (ArrayType(elementType1, containsNull1),
        ArrayType(elementType2, containsNull2)) =>
          ArrayType(
            compatibleType(failureOnConflict)(elementType1, elementType2),
            containsNull1 || containsNull2)
        case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) => DoubleType
        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          if (range + scale > 38) {
            DoubleType
          } else {
            DecimalType(range + scale, scale)
          }
        case (t1: DataType, t2: DecimalType) if isIntegral(t1) =>
          compatibleType(failureOnConflict)(DecimalTypeforType(t1), t2)
        case (t1: DecimalType, t2: DataType) if isIntegral(t2) =>
          compatibleType(failureOnConflict)(t1, DecimalTypeforType(t2))
        case (t1: DataType, StringType) if isIntegral(t1) => StringType
        case (StringType, t1: DataType) if isIntegral(t1) => StringType
        case (BooleanType, StringType) | (StringType, BooleanType) => StringType
        case (t1: DataType, StringType) if isDateOrTime(t1) => StringType
        case (StringType, t1: DataType) if isDateOrTime(t1) => StringType
        case (_, _) =>
          if (failureOnConflict) {
            throw new SchemaMappingException(s"Schema cannot be inferred")
          } else {
            StructType(StructField("InvalidType", StringType) :: Nil)
          }
      }
    }
  }

  def reflectSchema[T <: Product : TypeTag](): Option[StructType] = {
    typeOf[T] match {
      case x if x == typeOf[Nothing] => None
      case _ =>
        Some(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
    }
  }

  def reflectSchema[T](beanClass: Class[T]): StructType = {
    JavaTypeInference.inferDataType(beanClass)._1.asInstanceOf[StructType]
  }

  def isIntegral(dt: DataType): Boolean = {
    dt == LongType ||
      dt == IntegerType ||
      dt == ByteType ||
      dt == ShortType ||
      dt == FloatType ||
      dt == DoubleType
  }

  def isDateOrTime(dt: DataType): Boolean = dt == DateType || dt == TimestampType

  def isInvalidType(st: StructType): Boolean = st.fieldNames.contains("InvalidType")

}
