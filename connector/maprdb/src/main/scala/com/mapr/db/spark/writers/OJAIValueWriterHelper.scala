/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.writers

import java.nio.ByteBuffer

import com.mapr.db.spark.condition.DBQueryCondition
import com.mapr.db.spark.types.DBBinaryValue
import org.ojai.Document
import org.ojai.store.DocumentMutation

private[spark] sealed trait OJAIKey[T] extends Serializable {
  type Self
  def getValue(elem: T): Self
  def write(doc: Document, key: Self, table: Writer)
  def checkAndMutate(mutation: DocumentMutation,
                     queryCondition: DBQueryCondition,
                     key: Self,
                     table: TableCheckAndMutateWriter)
}

private[spark] object OJAIKey {
  implicit val ojaiStringKey = new OJAIKey[String] {
    override type Self = String
    override def getValue(elem: String) = elem
    override def write(doc: Document, key: String, table: Writer) =
      table.write(doc, key)
    override def checkAndMutate(mutation: DocumentMutation,
                                queryCondition: DBQueryCondition,
                                key: String,
                                table: TableCheckAndMutateWriter): Unit =
      table.write(mutation, queryCondition, key)
  }

  implicit val ojaibytebufferKey = new OJAIKey[ByteBuffer] {
    override type Self = ByteBuffer
    override def getValue(elem: ByteBuffer) = elem
    override def write(doc: Document, key: ByteBuffer, table: Writer) =
      table.write(doc, key)

    override def checkAndMutate(mutation: DocumentMutation,
                                queryCondition: DBQueryCondition,
                                key: ByteBuffer,
                                table: TableCheckAndMutateWriter): Unit =
      table.write(mutation, queryCondition, key)
  }

  implicit val ojaibinaryKey = new OJAIKey[DBBinaryValue] {
    override type Self = ByteBuffer
    override def getValue(elem: DBBinaryValue) = elem.getByteBuffer()
    override def write(doc: Document, key: ByteBuffer, table: Writer) =
      table.write(doc, key)
    override def checkAndMutate(mutation: DocumentMutation,
                                queryCondition: DBQueryCondition,
                                key: ByteBuffer,
                                table: TableCheckAndMutateWriter): Unit =
      table.write(mutation, queryCondition, key)
  }
}
