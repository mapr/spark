/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.utils

import java.io.ObjectInput
import java.nio.ByteBuffer
import com.mapr.db.exceptions.TableNotFoundException
import com.mapr.db.exceptions.TableExistsException
import com.mapr.db.impl.ConditionNode.RowkeyRange
import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.codec.BeanCodec
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.types.{DBArrayValue, DBBinaryValue, DBMapValue}
import com.mapr.db.spark.writers.OJAIKey
import com.mapr.fs.jni.MapRConstants
import com.mapr.org.apache.hadoop.hbase.util.Bytes
import org.ojai.Document
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

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
      tabDesc.setInsertionOrder(false)
      if (keys.isEmpty) {
        DBClient().createTable(tabDesc)
      } else {
        DBClient().createTable(tabDesc, keys.toArray)
      }

      (true, bulkMode)
    } else if (createTable) {
      throw new TableExistsException("Table: " + tableName + " already Exists")
    } else {
      if (bulkMode == true) isBulkLoad = DBClient().isBulkLoad(tableName)
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
    if (value.isInstanceOf[DBArrayValue[_]]) {
      value.asInstanceOf[DBArrayValue[_]].arr.map(_.asInstanceOf[AnyRef])
    } else {
      value.map(convertToScalaCollection(_))
    }
  }

  def convertToMap(value: Map[String, Any]): Map[String, AnyRef] = {
    if (value.isInstanceOf[DBMapValue]) {
      value.asInstanceOf[DBMapValue].value.map {
        case (k, v) => k -> v.asInstanceOf[AnyRef]
      }
    } else {
      value.map { case (k, v) => k -> convertToScalaCollection(v) }
    }
  }

  def convertToScalaCollection(value: Any): AnyRef = {
    if (value.isInstanceOf[DBMapValue]) {
      return value.asInstanceOf[DBMapValue].value.asJava.asInstanceOf[AnyRef]
    } else if (value.isInstanceOf[DBArrayValue[_]]) {
      return value.asInstanceOf[DBArrayValue[_]].arr.asJava.asInstanceOf[AnyRef]
    } else if (value.isInstanceOf[Map[_, _]]) {
      return value
        .asInstanceOf[Map[String, Any]]
        .map { case (k, v) => k -> convertToScalaCollection(v) }
        .asJava
        .asInstanceOf[AnyRef]
    } else if (value.isInstanceOf[Seq[Any]]) {
      return value
        .asInstanceOf[Seq[Any]]
        .map(convertToScalaCollection(_))
        .asJava
        .asInstanceOf[AnyRef]
    } else {
      return value.asInstanceOf[AnyRef]
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

    return result.asInstanceOf[OJAIKey[T]]
  }

  implicit class ClassTagOps[T](val classTag: ClassTag[T]) extends AnyVal {
    def <<:(other: ClassTag[_]): Boolean =
      classTag.runtimeClass.isAssignableFrom(other.runtimeClass)
  }
}
