/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD.partitioner

import java.nio.ByteBuffer

import com.mapr.db.impl.ConditionNode.RowkeyRange
import com.mapr.db.impl.IdCodec
import com.mapr.db.spark.types.DBBinaryValue
import com.mapr.ojai.store.impl.OjaiDocumentStore
import org.ojai.store.DocumentStore

trait OJAIKEY[T] extends Serializable {
  type Self
  def getValue(value: Any): Self
  def getTabletInfo(store: DocumentStore, value: Self)
  def getRange(splitkeys: (Self, Self)): RowkeyRange
  def getBytes(value: Any): Array[Byte]
  def getValueFromBinary(value: DBBinaryValue): Self
  def getclass() : String
}

object OJAIKEY {

  implicit def idkey: OJAIKEY[DBBinaryValue] = new OJAIKEY[DBBinaryValue] {
    override type Self = ByteBuffer
    override def getValue(value: Any): Self = value.asInstanceOf[DBBinaryValue].getByteBuffer()
    override def getTabletInfo(store: DocumentStore, value: Self) = {
      store.asInstanceOf[OjaiDocumentStore].getTable.getTabletInfo(value)
    }
    override def getRange(splitkeys: (Self, Self)): RowkeyRange = {
      if (splitkeys._1 == null) {
        if (splitkeys._2 == null) {
          new RowkeyRange(null, null)
        } else {
          new RowkeyRange(null, splitkeys._2.array())
        }
      } else {
        if (splitkeys._2 == null) {
          new RowkeyRange(splitkeys._1.array(), null)
        } else {
          new RowkeyRange(splitkeys._1.array(), splitkeys._2.array())
        }
      }
    }
    override def getBytes(value: Any): Array[Byte] =
      IdCodec.encodeAsBytes(value.asInstanceOf[DBBinaryValue].getByteBuffer())

    override def getValueFromBinary(value: DBBinaryValue) =
      IdCodec.decodeBinary(value.getByteBuffer())

    override def getclass(): String = "DBBinaryValue"
  }

  implicit def idbytebuff: OJAIKEY[ByteBuffer] = new OJAIKEY[ByteBuffer] {
    override type Self = ByteBuffer
    override def getValue(value: Any): Self = value.asInstanceOf[ByteBuffer]
    override def getTabletInfo(store: DocumentStore, value: Self) = {
      store.asInstanceOf[OjaiDocumentStore].getTable.getTabletInfo(value)
    }
    override def getRange(splitkeys: (Self, Self)): RowkeyRange = {
      if (splitkeys._1 == null) {
        if (splitkeys._2 == null) {
          new RowkeyRange(null, null)
        } else {
          new RowkeyRange(null, splitkeys._2.array())
        }
      } else {
        if (splitkeys._2 == null) {
          new RowkeyRange(splitkeys._1.array(), null)
        } else {
          new RowkeyRange(splitkeys._1.array(), splitkeys._2.array())
        }
      }
    }

    override def getBytes(value: Any): Array[Byte] =
      IdCodec.encodeAsBytes(value.asInstanceOf[ByteBuffer])

    override def getValueFromBinary(value: DBBinaryValue) =
      IdCodec.decodeBinary(value.getByteBuffer())

    override def getclass(): String = "ByteBuffer"
  }

  implicit def strkey: OJAIKEY[String] = new OJAIKEY[String] {
    override type Self = String
    override def getValue(value: Any): Self = value.asInstanceOf[Self]
    override def getTabletInfo(store: DocumentStore, value: Self) = {
      store.asInstanceOf[OjaiDocumentStore].getTable.getTabletInfo(value)
    }
    override def getRange(splitkeys: (Self, Self)): RowkeyRange =
      new RowkeyRange(IdCodec.encodeAsBytes(splitkeys._1), IdCodec.encodeAsBytes(splitkeys._2))
    override def getBytes(value: Any): Array[Byte] =
      IdCodec.encodeAsBytes(value.asInstanceOf[String])

    override def getValueFromBinary(value: DBBinaryValue) = IdCodec.decodeString(value.array())
    override def getclass(): String = "String"
  }
}
