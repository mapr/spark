/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.documentTypeUtils

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.types.{DBArrayValue, DBBinaryValue, DBMapValue}
import org.ojai.{Document, Value}
import org.ojai.exceptions.TypeException
import org.ojai.types._

sealed trait OJAIType[T] {
  type Self
  def getValue(doc: Document, fieldPath: String): Self
  def setValue(dc: OJAIDocument, name: String, value: T): Unit
}

object OJAIType {
  implicit val ojaianyref = new OJAIType[AnyRef] {
    type Self = AnyRef

    def getValue(doc: Document, fieldPath: String): Self = {
      val result = doc.getValue(fieldPath)
      if (result == null || result.getObject == null) {
        null
      } else if (result.getType == Value.Type.MAP) {
        new DBMapValue(result.getMap.asScala.toMap)
      } else if (result.getType == Value.Type.ARRAY) {
        new DBArrayValue(result.getList.asScala.toSeq)
      } else if (result.getType == Value.Type.BINARY) {
        new DBBinaryValue(result.getBinary)
      } else result.getObject
    }

    def setValue(dc: OJAIDocument, name: String, value: AnyRef): Unit = {
      throw new TypeException(
        "Invalid value of datatype " + value.getClass + " is set to the document")
    }
  }

  implicit val ojaiinteger = new OJAIType[Integer] {
    type Self = Integer
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getIntObj(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: Integer): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaiint = new OJAIType[Int] {
    type Self = Integer
    def getValue(doc: Document, fieldPath: String): Self = doc.getInt(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: Int): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaiDouble = new OJAIType[Double] {
    type Self = Double
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getDouble(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: Double): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaiDoubleObj = new OJAIType[java.lang.Double] {
    type Self = java.lang.Double
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getDoubleObj(fieldPath)
    def setValue(dc: OJAIDocument,
                 name: String,
                 value: java.lang.Double): Unit = dc.getDoc.set(name, value)
  }

  implicit val ojaifloat = new OJAIType[Float] {
    type Self = Float
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getFloat(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: Float): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaifloatObj = new OJAIType[java.lang.Float] {
    type Self = java.lang.Float
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getFloatObj(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: java.lang.Float): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojailong = new OJAIType[Long] {
    type Self = Long
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getLong(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: Long): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojailongObj = new OJAIType[java.lang.Long] {
    type Self = java.lang.Long
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getLongObj(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: java.lang.Long): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaidate = new OJAIType[ODate] {
    type Self = ODate
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getDate(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: ODate): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojatime = new OJAIType[OTime] {
    type Self = OTime
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getTime(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: OTime): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaitimestmp = new OJAIType[OTimestamp] {
    type Self = OTimestamp
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getTimestamp(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: OTimestamp): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaiintrvl = new OJAIType[OInterval] {
    type Self = OInterval
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getInterval(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: OInterval): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaiString = new OJAIType[String] {
    type Self = String
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getString(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: String): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaibool = new OJAIType[Boolean] {
    type Self = Boolean
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getBoolean(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: Boolean): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaiboolObj = new OJAIType[java.lang.Boolean] {
    type Self = java.lang.Boolean
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getBooleanObj(fieldPath)
    def setValue(dc: OJAIDocument,
                 name: String,
                 value: java.lang.Boolean): Unit = dc.getDoc.set(name, value)
  }

  implicit val ojaibyte = new OJAIType[Byte] {
    type Self = Byte
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getByte(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: Byte): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaibyteObj = new OJAIType[java.lang.Byte] {
    type Self = java.lang.Byte
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getByte(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: java.lang.Byte): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaishort = new OJAIType[Short] {
    type Self = Short
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getShort(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: Short): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaishortObj = new OJAIType[java.lang.Short] {
    type Self = java.lang.Short
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getShort(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: java.lang.Short): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaiBigDecimal = new OJAIType[BigDecimal] {
    type Self = BigDecimal
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getDecimal(fieldPath)
    def setValue(dc: OJAIDocument, name: String, value: BigDecimal): Unit =
      dc.getDoc.set(name, value.bigDecimal)

  }
  implicit val ojaiarrayvalue = new OJAIType[DBArrayValue[AnyRef]] {
    type Self = DBArrayValue[AnyRef]
    def getValue(doc: Document, fieldPath: String): Self = {
      val result = doc.getList(fieldPath)
      if (result == null) null else new DBArrayValue(result.asScala.toSeq)
    }
    def setValue(dc: OJAIDocument,
                 name: String,
                 value: DBArrayValue[AnyRef]): Unit =
      dc.getDoc.set(name, value.arr.asJava)
  }

  implicit val ojaimapstringvalue = new OJAIType[Map[String, AnyRef]] {
    type Self = DBMapValue
    def getValue(doc: Document, fieldPath: String): Self = {
      val result: java.util.Map[String, Object] = doc.getMap(fieldPath)

      if (result == null) null else new DBMapValue(result.asScala.toMap)
    }
    override def setValue(dc: OJAIDocument,
                          name: String,
                          value: Map[String, AnyRef]): Unit =
      dc.getDoc.set(name, value.asJava)
  }

  implicit val ojaiarrayanyref = new OJAIType[Seq[AnyRef]] {
    type Self = Seq[AnyRef]
    def getValue(doc: Document, fieldPath: String): Self = {
      val result: java.util.List[Object] = doc.getList(fieldPath)

      if (result == null) null else new DBArrayValue(result.asScala.toSeq)
    }
    override def setValue(dc: OJAIDocument,
                          name: String,
                          value: Seq[AnyRef]): Unit =
      dc.getDoc.set(name, value.asJava)
  }

  implicit val ojaiarrayany = new OJAIType[Seq[Any]] {
    type Self = DBArrayValue[AnyRef]
    def getValue(doc: Document, fieldPath: String): Self = {
      val result: java.util.List[Object] = doc.getList(fieldPath)

      if (result == null) null else new DBArrayValue(result.asScala.toSeq)
    }
    override def setValue(dc: OJAIDocument,
                          name: String,
                          value: Seq[Any]): Unit =
      dc.getDoc.set(name, value.map(_.asInstanceOf[AnyRef]).asJava)
  }

  implicit val ojaibinary = new OJAIType[DBBinaryValue] {
    type Self = DBBinaryValue
    def getValue(doc: Document, fieldPath: String): Self =
      new DBBinaryValue(doc.getBinary(fieldPath))
    override def setValue(dc: OJAIDocument,
                          name: String,
                          value: DBBinaryValue): Unit =
      dc.getDoc.set(name, value.getByteBuffer())
  }

  implicit val ojaibytebuffer = new OJAIType[ByteBuffer] {
    type Self = ByteBuffer
    def getValue(doc: Document, fieldPath: String): Self =
      doc.getBinary(fieldPath)
    override def setValue(dc: OJAIDocument,
                          name: String,
                          value: ByteBuffer): Unit =
      dc.getDoc.set(name, value)
  }

  implicit val ojaimapvalue = new OJAIType[DBMapValue] {
    type Self = DBMapValue

    def getValue(doc: Document, fieldPath: String): Self = {
      val result: java.util.Map[String, Object] = doc.getMap(fieldPath)
      if (result == null) null else new DBMapValue(result.asScala.toMap)
    }

    override def setValue(dc: OJAIDocument,
                          name: String,
                          value: DBMapValue): Unit =
      dc.getDoc.set(name, value.asJava)
  }
}
