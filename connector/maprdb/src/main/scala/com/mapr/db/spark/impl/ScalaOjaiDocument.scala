/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.impl

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.JavaConverters._

import com.mapr.db.impl.IdCodec
import com.mapr.db.rowcol.RowcolCodec
import com.mapr.db.spark.documentUtils.ScalaDocumentIterator
import com.mapr.db.spark.types.{DBArrayValue, DBBinaryValue, DBMapValue}
import com.mapr.db.spark.utils.{LoggingTrait, MapRDBUtils}
import com.mapr.db.util.ByteBufs
import org.ojai.{DocumentReader, FieldPath, Value}
import org.ojai.exceptions.DecodingException
import org.ojai.json.JsonOptions
import org.ojai.types._

/**
* This class implements scala's version of OJAIDocument.
* It encapsulates the org.ojai.Document and provides the functionality to
* the caller with relevant scala's types for ojai's java types.
*/
private[spark] abstract class ScalaOjaiDocument[B <: ScalaOjaiDocument[B]](
    @transient private var dc: org.ojai.Document)
    extends org.ojai.scala.Document
    with Externalizable
    with LoggingTrait {

  // constructor required for serialization.
  def this() {
    this(null)
  }

  def THIS: B

  lazy val getDoc = dc

  override def writeExternal(objectOutput: ObjectOutput): Unit = {
    val idbuff = IdCodec.encode(dc.getId)
    objectOutput.writeInt(idbuff.capacity())
    idbuff.order(ByteOrder.LITTLE_ENDIAN)
    objectOutput.write(idbuff.array(), 0, idbuff.capacity())
    val buff = RowcolCodec.encode(dc)
    buff.order(ByteOrder.LITTLE_ENDIAN)
    objectOutput.writeInt(buff.capacity())
    objectOutput.write(buff.array(), 0, buff.capacity())
    logDebug(
      "Serializing OJAI Document: bytes written:" + buff
        .capacity() + " bytes written for ID field: " + idbuff.capacity())
  }

  override def readExternal(objectinput: ObjectInput): Unit = {
    val idBufferSize = objectinput.readInt()
    val idbuffer = ByteBufs.allocate(idBufferSize)
    MapRDBUtils.readBytes(idbuffer, idBufferSize, objectinput)
    val bufferSize = objectinput.readInt()
    val buff: ByteBuffer = ByteBufs.allocate(bufferSize)
    MapRDBUtils.readBytes(buff, bufferSize, objectinput)
    dc = RowcolCodec.decode(buff, idbuffer, false, false, true)
    logDebug(
      s"Document Deserialized : bytes read: $bufferSize bytes read for ID field: $idBufferSize")
  }

  override def toString: String = dc.asJsonString()

  override def setId(id: Value): B = {
    this.dc.setId(id)
    THIS
  }

  override def getId(): Value = this.dc.getId

  override def setId(_id: String): B = {
    this.dc.setId(_id)
    THIS
  }

  override def getIdString(): String = {
    this.dc.getIdString
  }

  override def setId(_id: ByteBuffer): B = {
    this.dc.setId(_id)
    THIS
  }

  def setId(_id: DBBinaryValue): B = {
    this.dc.setId(_id.getByteBuffer())
    THIS
  }

  override def getIdBinary: ByteBuffer = this.dc.getIdBinary

  def getIdBinarySerializable: DBBinaryValue = new DBBinaryValue(this.dc.getIdBinary)

  override def isReadOnly: Boolean = this.isReadOnly()


  override def size: Int = this.dc.size


  @throws(classOf[DecodingException])
  override def toJavaBean[T](beanClass: Class[T]): T = this.dc.toJavaBean(beanClass)

  override def empty(): B = {
    this.dc.empty
    THIS
  }

  override def set(fieldPath: String, value: String): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: String): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Boolean): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Boolean): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Byte): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Byte): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Short): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Short): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Integer): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Integer): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Long): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Long): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Float): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Float): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Double): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Double): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: BigDecimal): B = {
    this.dc.set(fieldPath, value.bigDecimal)
    THIS
  }

  override def set(fieldPath: FieldPath, value: BigDecimal): B = {
    this.dc.set(fieldPath, value.bigDecimal)
    THIS
  }

  override def set(fieldPath: String, value: OTime): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: OTime): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: ODate): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: ODate): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: OTimestamp): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: OTimestamp): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: OInterval): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: OInterval): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String,
                   value: Seq[Byte],
                   off: Integer,
                   len: Integer): B = {
    this.dc.set(fieldPath, value.asJava)
    THIS
  }

  override def set(fieldPath: FieldPath,
                   value: Seq[Byte],
                   off: Integer,
                   len: Integer): B = {
    this.dc.set(fieldPath, value.asJava)
    THIS
  }

  override def set(fieldPath: String, value: ByteBuffer): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: ByteBuffer): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Map[String, _ <: AnyRef]): B = {
    this.dc.set(fieldPath, value.asJava)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Map[String, _ <: AnyRef]): B = {
    this.dc.set(fieldPath, value.asJava)
    THIS
  }

  override def set(fieldPath: String, value: org.ojai.scala.Document): B = {
    this.dc.set(fieldPath, value.asInstanceOf[ScalaOjaiDocument[_]].dc)
    THIS
  }

  override def set(fieldPath: FieldPath, value: org.ojai.scala.Document): B = {
    this.dc.set(fieldPath, value.asInstanceOf[ScalaOjaiDocument[_]].dc)
    THIS
  }

  override def set(fieldPath: String, value: Value): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Value): B = {
    this.dc.set(fieldPath, value)
    THIS
  }

  override def set(fieldPath: String, value: Seq[_ <: AnyRef]): B = {
    this.dc.set(fieldPath, value.asJava)
    THIS
  }

  override def set(fieldPath: FieldPath, value: Seq[_ <: AnyRef]): B = {
    this.dc.set(fieldPath, value.asJava)
    THIS
  }

  override def setArray(fieldPath: String, values: AnyRef*): B = {
    this.dc.setArray(fieldPath, values: _*)
    THIS
  }

  override def setArray(fieldPath: FieldPath, values: AnyRef*): B = {
    this.dc.setArray(fieldPath, values: _*)
    THIS
  }

  def setArray(fieldPath: String, values: Array[Boolean]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: FieldPath, values: Array[Boolean]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: String, values: Array[Byte]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: FieldPath, values: Array[Byte]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: String, values: Array[Short]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: FieldPath, values: Array[Short]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: String, values: Array[Int]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: FieldPath, values: Array[Int]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: String, values: Array[Long]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: FieldPath, values: Array[Long]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: String, values: Array[Float]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: FieldPath, values: Array[Float]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: String, values: Array[Double]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: FieldPath, values: Array[Double]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: String, values: Array[String]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  def setArray(fieldPath: FieldPath, values: Array[String]): B = {
    this.dc.setArray(fieldPath, values)
    THIS
  }

  override def setNull(fieldPath: String): B = {
    this.dc.setNull(fieldPath)
    THIS
  }

  override def setNull(fieldPath: FieldPath): B = {
    this.dc.setNull(fieldPath)
    THIS
  }

  override def delete(fieldPath: String): B = {
    this.dc.delete(fieldPath)
    THIS
  }

  override def delete(fieldPath: FieldPath): B = {
    this.dc.delete(fieldPath)
    THIS
  }

  override def getString(fieldPath: String): String = {
    this.dc.getString(fieldPath)
  }

  override def getString(fieldPath: FieldPath): String = {
    this.dc.getString(fieldPath)
  }

  override def getBoolean(fieldPath: String): Boolean = {
    this.dc.getBoolean(fieldPath)
  }

  override def getBoolean(fieldPath: FieldPath): Boolean = {
    this.dc.getBoolean(fieldPath)
  }

  override def getBooleanObj(fieldPath: String): java.lang.Boolean = {
    this.dc.getBooleanObj(fieldPath)
  }

  override def getBooleanObj(fieldPath: FieldPath): java.lang.Boolean = {
    this.dc.getBooleanObj(fieldPath)
  }

  override def getByte(fieldPath: String): Byte = {
    this.dc.getByte(fieldPath)
  }

  override def getByte(fieldPath: FieldPath): Byte = {
    this.dc.getByte(fieldPath)
  }

  override def getByteObj(fieldPath: String): java.lang.Byte = {
    this.dc.getByteObj(fieldPath)
  }

  override def getByteObj(fieldPath: FieldPath): java.lang.Byte = {
    this.dc.getByteObj(fieldPath)
  }

  override def getShort(fieldPath: String): Short = {
    this.dc.getShort(fieldPath)
  }

  override def getShort(fieldPath: FieldPath): Short = {
    this.dc.getShort(fieldPath)
  }

  override def getShortObj(fieldPath: String): java.lang.Short = {
    this.dc.getShortObj(fieldPath)
  }

  override def getShortObj(fieldPath: FieldPath): java.lang.Short = {
    this.dc.getShortObj(fieldPath)
  }

  override def getInt(fieldPath: String): Int = {
    this.dc.getInt(fieldPath)
  }

  override def getInt(fieldPath: FieldPath): Int = {
    this.dc.getInt(fieldPath)
  }

  override def getIntObj(fieldPath: String): Integer = {
    this.dc.getIntObj(fieldPath)
  }

  override def getIntObj(fieldPath: FieldPath): Integer = {
    this.dc.getIntObj(fieldPath)
  }

  override def getLong(fieldPath: String): Long = {
    this.dc.getLong(fieldPath)
  }

  override def getLong(fieldPath: FieldPath): Long = {
    this.dc.getLong(fieldPath)
  }

  override def getLongObj(fieldPath: String): java.lang.Long = {
    this.dc.getLongObj(fieldPath)
  }

  override def getLongObj(fieldPath: FieldPath): java.lang.Long = {
    this.dc.getLongObj(fieldPath)
  }

  override def getFloat(fieldPath: String): Float = {
    this.dc.getFloat(fieldPath)
  }

  override def getFloat(fieldPath: FieldPath): Float = {
    this.dc.getFloat(fieldPath)
  }

  override def getFloatObj(fieldPath: String): java.lang.Float = {
    this.dc.getFloatObj(fieldPath)
  }

  override def getFloatObj(fieldPath: FieldPath): java.lang.Float = {
    this.dc.getFloatObj(fieldPath)
  }

  override def getDouble(fieldPath: String): Double = {
    this.dc.getDouble(fieldPath)
  }

  override def getDouble(fieldPath: FieldPath): Double = {
    this.dc.getDouble(fieldPath)
  }

  override def getDoubleObj(fieldPath: String): java.lang.Double = {
    this.dc.getDoubleObj(fieldPath)
  }

  override def getDoubleObj(fieldPath: FieldPath): java.lang.Double = {
    this.dc.getDoubleObj(fieldPath)
  }

  override def getDecimal(fieldPath: String): BigDecimal = {
    BigDecimal.javaBigDecimal2bigDecimal(this.dc.getDecimal(fieldPath))
  }

  override def getDecimal(fieldPath: FieldPath): BigDecimal = {
    BigDecimal.javaBigDecimal2bigDecimal(this.dc.getDecimal(fieldPath))
  }

  override def getTime(fieldPath: String): OTime = {
    this.dc.getTime(fieldPath)
  }

  override def getTime(fieldPath: FieldPath): OTime = {
    this.dc.getTime(fieldPath)
  }

  override def getDate(fieldPath: String): ODate = {
    this.dc.getDate(fieldPath)
  }

  override def getDate(fieldPath: FieldPath): ODate = {
    this.dc.getDate(fieldPath)
  }

  override def getTimestamp(fieldPath: String): OTimestamp = {
    this.dc.getTimestamp(fieldPath)
  }

  override def getTimestamp(fieldPath: FieldPath): OTimestamp = {
    this.dc.getTimestamp(fieldPath)
  }

  override def getBinary(fieldPath: String): ByteBuffer = {
    this.dc.getBinary(fieldPath)
  }

  def getBinarySerializable(fieldPath: String): DBBinaryValue = {
    new DBBinaryValue(this.dc.getBinary(fieldPath))
  }

  override def getBinary(fieldPath: FieldPath): ByteBuffer = {
    this.dc.getBinary(fieldPath)
  }

  def getBinarySerializable(fieldPath: FieldPath): DBBinaryValue = {
    new DBBinaryValue(this.dc.getBinary(fieldPath))
  }

  override def getInterval(fieldPath: String): OInterval = {
    this.dc.getInterval(fieldPath)
  }

  override def getInterval(fieldPath: FieldPath): OInterval = {
    this.dc.getInterval(fieldPath)
  }

  override def getValue(fieldPath: String): Value = {
    this.dc.getValue(fieldPath)
  }

  override def getValue(fieldPath: FieldPath): Value = {
    this.dc.getValue(fieldPath)
  }

  override def getMap(fieldPath: String): Map[String, AnyRef] = {
    val result: java.util.Map[String, Object] = this.dc.getMap(fieldPath)
    if (result == null) null else new DBMapValue(result.asScala.toMap)
  }

  override def getMap(fieldPath: FieldPath): Map[String, AnyRef] = {
    val result: java.util.Map[String, Object] = this.dc.getMap(fieldPath)
    if (result == null) null else new DBMapValue(result.asScala.toMap)
  }

  override def getList(fieldPath: String): Seq[AnyRef] = {
    val result: java.util.List[Object] = this.dc.getList(fieldPath)
    if (result == null) null else new DBArrayValue(result.asScala)
  }

  override def getList(fieldPath: FieldPath): Seq[AnyRef] = {
    val result = this.dc.getList(fieldPath)
    if (result == null) null else new DBArrayValue(result.asScala)
  }

  override def asJsonString(): String = {
    this.dc.asJsonString
  }

  override def asJsonString(options: JsonOptions): String = {
    this.dc.asJsonString(options)
  }

  override def asReader(): DocumentReader = {
    this.dc.asReader()
  }

  override def asReader(fieldPath: String): DocumentReader = {
    this.dc.asReader(fieldPath)
  }

  override def asReader(fieldPath: FieldPath): DocumentReader = {
    this.dc.asReader(fieldPath)
  }

  override def asMap(): Map[String, AnyRef] = {
    new DBMapValue(dc.asMap().asScala.toMap)
  }

  override def iterator: ScalaDocumentIterator = new ScalaDocumentIterator(this.dc.iterator)
}
