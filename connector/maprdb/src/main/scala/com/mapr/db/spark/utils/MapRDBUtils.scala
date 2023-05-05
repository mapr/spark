/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.utils

import java.io.ObjectInput
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.mapr.db.exceptions.{TableExistsException, TableNotFoundException}
import com.mapr.db.impl.ConditionNode.RowkeyRange
import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.codec.BeanCodec
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.types.{DBArrayValue, DBBinaryValue, DBMapValue}
import com.mapr.db.spark.writers.OJAIKey
import com.mapr.fs.jni.MapRConstants
import com.mapr.org.apache.hadoop.hbase.util.Bytes
import org.ojai.Document

private[spark] object MapRDBUtils {
  def checkOrCreateTable(tableName: String,
                         bulkMode: Boolean,
                         createTable: Boolean,
                         keys: Seq[org.ojai.Value]): (Boolean, Boolean) = {
    var isBulkLoad: Boolean = bulkMode
    if (!DBClient().tableExists(tableName)) {
      if (!createTable) {
        throw new TableNotFoundException("Table: " + tableName + " not found")
      }

      val tabDesc = DBClient().newTableDescriptor()
      tabDesc.setAutoSplit(true)
      tabDesc.setPath(tableName)
      tabDesc.setBulkLoad(bulkMode)
      if (keys.isEmpty) {
        DBClient().createTable(tabDesc)
      } else {
        DBClient().createTable(tabDesc, keys.toArray)
      }

      (true, bulkMode)
    } else if (createTable) {
      throw new TableExistsException("Table: " + tableName + " already Exists")
    } else {
      if (bulkMode) isBulkLoad = DBClient().isBulkLoad(tableName)
      (false, isBulkLoad)
    }
  }

  def setBulkLoad(tableName: String, bulkMode: Boolean): Unit = {
    val desc = DBClient().getTableDescriptor(tableName)
    desc.setBulkLoad(bulkMode)
    DBClient().alterTable(desc)
  }

  def containsRow(row: Array[Byte], rowkeyRange: RowkeyRange): Boolean = {
    Bytes.compareTo(row, rowkeyRange.getStartRow) >= 0 &&
      (Bytes.compareTo(row, rowkeyRange.getStopRow) < 0 ||
        Bytes.equals(rowkeyRange.getStopRow, MapRConstants.EMPTY_BYTE_ARRAY))
  }

  def toBeanClass[T](doc: Document, beanClass: Class[T]): T = {
    if (beanClass.getSimpleName.equals("OJAIDocument")) {
      MapRDBSpark.newDocument(doc).asInstanceOf[T]
    } else {
      BeanCodec.encode[T](doc.asReader(), beanClass)
    }
  }

  def readBytes(buff: ByteBuffer,
                bufferSize: Int,
                objectInput: ObjectInput): Unit = {
    val byteArray = new Array[Byte](bufferSize)
    var readbytes = objectInput.read(byteArray, 0, bufferSize)
    buff.put(byteArray, 0, readbytes)
    var remaining = bufferSize - readbytes

    while (remaining > 0) {
      val read = objectInput.read(byteArray, readbytes, remaining)
      buff.put(byteArray, readbytes, read)
      readbytes += read
      remaining -= read
    }
    buff.flip()
  }

  def convertToSeq(value: Seq[Any]): Seq[AnyRef] = {
    value match {
      case value1: DBArrayValue[_] =>
        value1.arr.map(_.asInstanceOf[AnyRef])
      case _ =>
        value.map(convertToScalaCollection)
    }
  }

  def convertToMap(value: Map[String, Any]): Map[String, AnyRef] = {
    value match {
      case value1: DBMapValue =>
        value1.value.map {
          case (k, v) => k -> v.asInstanceOf[AnyRef]
        }
      case _ =>
        value.map { case (k, v) => k -> convertToScalaCollection(v) }
    }
  }

  def convertToScalaCollection(value: Any): AnyRef = {
    value match {
      case value1: DBMapValue =>
        value1.value.asJava.asInstanceOf[AnyRef]
      case value1: DBArrayValue[_] =>
        value1.arr.asJava.asInstanceOf[AnyRef]
      case _: Map[_, _] =>
        value
          .asInstanceOf[Map[String, Any]]
          .map { case (k, v) => k -> convertToScalaCollection(v) }
          .asJava
          .asInstanceOf[AnyRef]
      case seq: Seq[Any] =>
        seq
          .map(convertToScalaCollection)
          .asJava
          .asInstanceOf[AnyRef]
      case _ =>
        value.asInstanceOf[AnyRef]
    }
  }

  def getOjaiKey[T: ClassTag](): OJAIKey[T] = {
    import reflect._
    val result = classTag[T] match {
      case a if a <<: classTag[String] =>
        return OJAIKey.ojaiStringKey.asInstanceOf[OJAIKey[T]]
      case a if a <<: classTag[ByteBuffer] =>
        return OJAIKey.ojaibytebufferKey.asInstanceOf[OJAIKey[T]]
      case a if a <<: classTag[DBBinaryValue] =>
        return OJAIKey.ojaibinaryKey.asInstanceOf[OJAIKey[T]]
      case _ =>
        throw new RuntimeException(
          "Key with type:" + classTag[T].runtimeClass + " is not supported")
    }

    result.asInstanceOf[OJAIKey[T]]
  }

  implicit class ClassTagOps[T](val classTag: ClassTag[T]) extends AnyVal {
    def <<:(other: ClassTag[_]): Boolean =
      classTag.runtimeClass.isAssignableFrom(other.runtimeClass)
  }
}
