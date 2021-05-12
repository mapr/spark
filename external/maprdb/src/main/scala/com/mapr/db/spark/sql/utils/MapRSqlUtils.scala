/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql.utils

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import com.mapr.db.rowcol.DBValueBuilderImpl
import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.exceptions.SchemaMappingException
import com.mapr.db.spark.impl.OJAIDocument
import org.ojai.DocumentReader
import org.ojai.types.{ODate, OInterval, OTimestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._

object MapRSqlUtils {

  def documentToRow(document: OJAIDocument, schema: StructType): Row = {
    convertObject(document.getDoc.asReader(), schema)
  }

  def documentsToRow(
      documents: Iterator[DocumentReader],
      schema: StructType,
      requiredColumns: Array[String] = Array.empty[String]): Iterator[Row] = {
    documents.flatMap(record =>
      convertRootField(record, schema) match {
        case row: Row => row :: Nil
        case array: ArrayData =>
          if (array.numElements == 0) Nil
          else array.toArray[Row](schema)
    })
  }

  def convertRootField(docReader: DocumentReader, schema: DataType): Any = {
    val event = docReader.next()
    (event, schema) match {
      case (DocumentReader.EventType.START_ARRAY, at @ ArrayType(st, _)) =>
        convertArray(docReader, at)
      case (DocumentReader.EventType.START_MAP, st: StructType) =>
        convertObject(docReader, st)
      case _ =>
        convertField(event, docReader, schema, docReader.getFieldName)
    }
  }

  private def convertObject(documentReader: DocumentReader, schema: StructType): Row = {
    val values = ArrayBuffer.fill[Any](schema.fields.size)(null)
    var event: DocumentReader.EventType = null
    event = documentReader.next
    while (event != null && event.compareTo(DocumentReader.EventType.END_MAP) != 0) {
      val fieldName = documentReader.getFieldName
      if (schema.fieldNames.contains(fieldName)) {
        val index = schema.fieldIndex(fieldName)
        values.update(index, convertField(event, documentReader, schema(index).dataType, fieldName))
      }
      event = documentReader.next
    }
    new GenericRowWithSchema(values.toArray, schema.asInstanceOf[StructType])
  }

  private def convertArray(documentReader: DocumentReader,
                           array: ArrayType): Any = {
    val values = ArrayBuffer.empty[Any]
    var event: DocumentReader.EventType = null
    val fieldName = documentReader.getFieldName
    event = documentReader.next
    while (event != null && event.compareTo(DocumentReader.EventType.END_ARRAY) != 0) {
      values += convertField(event,
                             documentReader,
                             array.elementType,
                             fieldName)
      event = documentReader.next
    }
    values.toArray
  }

  private def convertMap(documentReader: DocumentReader,
                         mapType: MapType): Any = {
    val keys = ArrayBuffer.empty[String]
    val values = ArrayBuffer.empty[Any]
    var event: DocumentReader.EventType = null
    event = documentReader.next
    while (event != null && event.compareTo(DocumentReader.EventType.END_MAP) != 0) {
      keys += documentReader.getFieldName
      values += convertField(event,
                             documentReader,
                             mapType.valueType,
                             documentReader.getFieldName)
      event = documentReader.next
    }
    ArrayBasedMapData.toScalaMap(keys.toArray, values.toArray)
  }

  def convertField(event: DocumentReader.EventType,
                   documentReader: DocumentReader,
                   schema: DataType,
                   fieldName: String): Any = {
    (event, schema) match {
      case (DocumentReader.EventType.START_ARRAY, at @ ArrayType(st, _)) =>
        convertArray(documentReader, at)
      case (DocumentReader.EventType.START_MAP,
            mt @ MapType(StringType, kt, _)) =>
        convertMap(documentReader, mt)
      case (DocumentReader.EventType.START_MAP, st: StructType) =>
        convertObject(documentReader, schema.asInstanceOf[StructType])
      case (DocumentReader.EventType.NULL | null, _) => null
      case (DocumentReader.EventType.STRING, StringType) =>
        documentReader.getString
      case (DocumentReader.EventType.STRING, BinaryType) =>
        documentReader.getString.getBytes
      case (DocumentReader.EventType.STRING, DateType) =>
        new java.sql.Date(
          ODate.parse(documentReader.getString).toDate.getTime)
      case (DocumentReader.EventType.STRING, TimestampType) =>
        new java.sql.Timestamp(
          OTimestamp.parse(documentReader.getString).getMillis)
      case (DocumentReader.EventType.STRING, DoubleType) =>
        documentReader.getString.toDouble
      case (DocumentReader.EventType.STRING, FloatType) =>
        documentReader.getString.toFloat
      case (DocumentReader.EventType.STRING, IntegerType) =>
        documentReader.getString.toInt
      case (DocumentReader.EventType.STRING, LongType) =>
        documentReader.getString.toLong
      case (DocumentReader.EventType.STRING, ByteType) =>
        documentReader.getString.toByte
      case (DocumentReader.EventType.STRING, ShortType) =>
        documentReader.getString.toShort
      case (DocumentReader.EventType.STRING, dt: DecimalType) =>
        Decimal(documentReader.getString.toFloat)
      case (DocumentReader.EventType.DATE, DateType) =>
        new java.sql.Date(documentReader.getDate.toDate.getTime)
      case (DocumentReader.EventType.DATE, TimestampType) =>
        new java.sql.Timestamp(documentReader.getDate.toDate.getTime)
      case (DocumentReader.EventType.DATE, StringType) =>
        documentReader.getDate.toString
      case (DocumentReader.EventType.TIME, TimestampType) =>
        new java.sql.Timestamp(documentReader.getTime.toDate.getTime)
      case (DocumentReader.EventType.TIME, StringType) =>
        documentReader.getTime.toString
      case (DocumentReader.EventType.TIME, DateType) =>
        new java.sql.Date(documentReader.getTime.getMilliSecond)
      case (DocumentReader.EventType.TIMESTAMP, TimestampType) =>
        new java.sql.Timestamp(documentReader.getTimestampLong)
      case (DocumentReader.EventType.TIMESTAMP, StringType) =>
        documentReader.getTimestamp.toString
      case (DocumentReader.EventType.TIMESTAMP, DateType) =>
        new java.sql.Date(documentReader.getTimestampLong)
      case (DocumentReader.EventType.INT, IntegerType) =>
        documentReader.getInt
      case (DocumentReader.EventType.INT, FloatType) =>
        documentReader.getInt.toFloat
      case (DocumentReader.EventType.INT, StringType) =>
        documentReader.getInt.toString
      case (DocumentReader.EventType.INT, LongType) =>
        documentReader.getInt.toLong
      case (DocumentReader.EventType.INT, DoubleType) =>
        documentReader.getInt.toDouble
      case (DocumentReader.EventType.INT, TimestampType) =>
        new java.sql.Timestamp(documentReader.getInt)
      case (DocumentReader.EventType.FLOAT, FloatType) =>
        documentReader.getFloat
      case (DocumentReader.EventType.FLOAT, DoubleType) =>
        documentReader.getFloat.toDouble
      case (DocumentReader.EventType.FLOAT, dt: DecimalType) =>
        Decimal(documentReader.getFloat)
      case (DocumentReader.EventType.FLOAT, st: StringType) =>
        documentReader.getFloat.toString
      case (DocumentReader.EventType.DOUBLE, DoubleType) =>
        documentReader.getDouble
      case (DocumentReader.EventType.DOUBLE, dt: DecimalType) =>
        Decimal(documentReader.getDouble)
      case (DocumentReader.EventType.DOUBLE, st: StringType) =>
        documentReader.getDouble.toString
      case (DocumentReader.EventType.LONG, LongType) =>
        documentReader.getLong
      // Converting from LONG to Double can loose precision for some values.
      case (DocumentReader.EventType.LONG, DoubleType) =>
        documentReader.getLong.toDouble
      case (DocumentReader.EventType.LONG, TimestampType) =>
        new java.sql.Timestamp(documentReader.getLong)
      case (DocumentReader.EventType.LONG, StringType) =>
        documentReader.getLong.toString
      case (DocumentReader.EventType.BYTE, ByteType) =>
        documentReader.getByte
      case (DocumentReader.EventType.BYTE, ShortType) =>
        documentReader.getByte.toShort
      case (DocumentReader.EventType.BYTE, IntegerType) =>
        documentReader.getByte.toInt
      case (DocumentReader.EventType.BYTE, LongType) =>
        documentReader.getByte.toLong
      case (DocumentReader.EventType.BYTE, FloatType) =>
        documentReader.getByte.toFloat
      case (DocumentReader.EventType.BYTE, DoubleType) =>
        documentReader.getByte.toDouble
      case (DocumentReader.EventType.BYTE, dt: DecimalType) =>
        Decimal(documentReader.getByte)
      case (DocumentReader.EventType.BYTE, StringType) =>
        documentReader.getByte.toString
      case (DocumentReader.EventType.SHORT, ShortType) =>
        documentReader.getShort
      case (DocumentReader.EventType.SHORT, IntegerType) =>
        documentReader.getShort.toInt
      case (DocumentReader.EventType.SHORT, LongType) =>
        documentReader.getShort.toLong
      case (DocumentReader.EventType.SHORT, FloatType) =>
        documentReader.getShort.toFloat
      case (DocumentReader.EventType.SHORT, DoubleType) =>
        documentReader.getShort.toDouble
      case (DocumentReader.EventType.SHORT, dt: DecimalType) =>
        Decimal(documentReader.getShort)
      case (DocumentReader.EventType.SHORT, StringType) =>
        documentReader.getShort.toString
      case (DocumentReader.EventType.BINARY, BinaryType) =>
        documentReader.getBinary.array()
      case (DocumentReader.EventType.BINARY, StringType) =>
        documentReader.getBinary.array().map(_.toChar)
      case (DocumentReader.EventType.DECIMAL, FloatType) =>
        documentReader.getDecimal.floatValue()
      case (DocumentReader.EventType.DECIMAL, DoubleType) =>
        documentReader.getDecimal.doubleValue()
      case (DocumentReader.EventType.DECIMAL, StringType) =>
        documentReader.getDecimal.toString
      case (DocumentReader.EventType.DECIMAL, LongType) =>
        documentReader.getDecimalValueAsLong
      case (DocumentReader.EventType.DECIMAL, IntegerType) =>
        documentReader.getDecimalValueAsInt
      case (DocumentReader.EventType.BOOLEAN, BooleanType) =>
        documentReader.getBoolean
      case (DocumentReader.EventType.BOOLEAN, StringType) =>
        documentReader.getBoolean.toString
      case (token, dataType) =>
        if (isInvalidType(dataType)) {
          throw new SchemaMappingException(
            s"Schema cannot be inferred for the column {$fieldName}")
        } else {
          throw new SchemaMappingException(
            s"Failed to parse a value for data type $dataType (current token: $token)")
        }
    }
  }

  def isInvalidType(dt: DataType): Boolean = dt match {
    case dt: StructType
        if dt.size == 1 && dt.map(_.name).contains("InvalidType") =>
      true
    case _ => false
  }

  def rowToDocument(row: Row): OJAIDocument = {
    val document = MapRDBSpark.newDocument(DBClient().newDocument())
    row.schema.fields.zipWithIndex.foreach({
      case (field, i) if !row.isNullAt(i) =>
        document.set(field.name, convertToDataType(row.get(i), field.dataType))
      case (field, i) if row.isNullAt(i) => document.setNull(field.name)
    })
    document
  }

  def convertToDataType(element: Any, elementType: DataType): org.ojai.Value = {
    elementType match {
      case (mapType: StructType) =>
        val map = rowToDocument(element.asInstanceOf[Row]).getDoc
        DBValueBuilderImpl.KeyValueBuilder.initFromObject(map)
      case (arrayType: ArrayType) =>
        val seq = element.asInstanceOf[Seq[Any]]
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          seq
            .map(elem => convertToDataType(elem, arrayType.elementType))
            .asJava)
      case (mpType: MapType) =>
        val map = element.asInstanceOf[Map[String, Any]]
        DBValueBuilderImpl.KeyValueBuilder.initFrom(map.asJava)
      case (BinaryType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          ByteBuffer.wrap(element.asInstanceOf[Array[Byte]]))
      case (BooleanType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          element.asInstanceOf[Boolean])
      case (DateType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          new ODate(element.asInstanceOf[java.sql.Date]))
      case (TimestampType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          new OTimestamp(element.asInstanceOf[java.sql.Timestamp].getTime))
      case (NullType) => DBValueBuilderImpl.KeyValueBuilder.initFromNull()
      case (DoubleType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          element.asInstanceOf[Double])
      case (IntegerType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          element.asInstanceOf[Integer])
      case (LongType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(element.asInstanceOf[Long])
      case (StringType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          element.asInstanceOf[String])
      case (CalendarIntervalType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          element.asInstanceOf[OInterval])
      case (DecimalType()) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(
          element.asInstanceOf[java.math.BigDecimal])
      case (FloatType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(element.asInstanceOf[Float])
      case (ShortType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(element.asInstanceOf[Short])
      case (ByteType) =>
        DBValueBuilderImpl.KeyValueBuilder.initFrom(element.asInstanceOf[Byte])
      case _ =>
        throw new RuntimeException(
          s"Cannot convert $elementType of a row to OjaiDocument's type")
    }
  }
}
