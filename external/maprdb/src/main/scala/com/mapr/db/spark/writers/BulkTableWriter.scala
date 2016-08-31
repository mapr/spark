/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.writers

import java.nio.ByteBuffer

import com.mapr.db.mapreduce.BulkLoadRecordWriter
import com.mapr.db.rowcol.DBValueBuilderImpl
import org.ojai.Document

private[spark] case class BulkTableWriter(@transient table: BulkLoadRecordWriter) extends Writer {

  def write(doc: Document, key: ByteBuffer): Unit = {
    table.write(DBValueBuilderImpl.KeyValueBuilder.initFrom(key), doc)
  }

  def write(doc: Document, key: String): Unit = {
    table.write(DBValueBuilderImpl.KeyValueBuilder.initFrom(key), doc)
  }

  def write(doc: Document, key: org.ojai.Value): Unit = {
    table.write(key, doc)
  }

  def close(): Unit = {
    table.close(null)
  }
}