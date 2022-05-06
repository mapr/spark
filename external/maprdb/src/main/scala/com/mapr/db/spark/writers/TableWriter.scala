/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.writers

import java.nio.ByteBuffer

import com.mapr.db.rowcol.DBValueBuilderImpl
import com.mapr.db.spark.condition.DBQueryCondition
import org.ojai.Document
import org.ojai.store.{DocumentMutation, DocumentStore}

private[spark] case class TableInsertOrReplaceWriter(
    @transient table: DocumentStore)
    extends Writer {

  def write(doc: Document, key: ByteBuffer): Unit = {
    write(doc, DBValueBuilderImpl.KeyValueBuilder.initFrom(key))
  }

  def write(doc: Document, key: String): Unit = {
    write(doc, DBValueBuilderImpl.KeyValueBuilder.initFrom(key))
  }

  def write(doc: Document, key: org.ojai.Value): Unit = {
    table.insertOrReplace(doc.setId(key))
  }

  def close(): Unit = {
    table.flush()
    table.close()
  }
}

private[spark] case class TableInsertWriter(@transient table: DocumentStore)
    extends Writer {

  def write(doc: Document, key: ByteBuffer): Unit = {
    write(doc, DBValueBuilderImpl.KeyValueBuilder.initFrom(key))
  }

  def write(doc: Document, key: String): Unit = {
    write(doc, DBValueBuilderImpl.KeyValueBuilder.initFrom(key))
  }

  def write(doc: Document, key: org.ojai.Value): Unit = {
    table.insert(doc.setId(key))
  }

  def close(): Unit = {
    table.flush()
    table.close()
  }
}

private[spark] case class TableDeleteWriter(@transient table: DocumentStore)
  extends Writer {

  def write(doc: Document, key: ByteBuffer): Unit = {
    write(doc, DBValueBuilderImpl.KeyValueBuilder.initFrom(key))
  }

  def write(doc: Document, key: String): Unit = {
    write(doc, DBValueBuilderImpl.KeyValueBuilder.initFrom(key))
  }

  def write(doc: Document, key: org.ojai.Value): Unit = {
    table.delete(doc.setId(key))
  }

  def close(): Unit = {
    table.flush()
    table.close()
  }
}

private[spark] case class TableCheckAndMutateWriter(
    @transient table: DocumentStore) {

  def write(mutation: DocumentMutation,
            queryCondition: DBQueryCondition,
            key: ByteBuffer): Unit = {
    write(mutation,
          queryCondition,
          DBValueBuilderImpl.KeyValueBuilder.initFrom(key))
  }

  def write(mutation: DocumentMutation,
            queryCondition: DBQueryCondition,
            key: String): Unit = {
    write(mutation,
          queryCondition,
          DBValueBuilderImpl.KeyValueBuilder.initFrom(key))
  }

  def write(mutation: DocumentMutation,
            queryCondition: DBQueryCondition,
            key: org.ojai.Value): Unit = {
    table.checkAndUpdate(key, queryCondition.condition, mutation)
  }

  def close(): Unit = {
    table.flush()
    table.close()
  }
}
