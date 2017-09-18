/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.writers

import java.nio.ByteBuffer
import org.ojai.Document
import com.mapr.db.mapreduce.BulkLoadRecordWriter
import com.mapr.db.rowcol.DBValueBuilderImpl

private[spark] case class BulkTableWriter(@transient table: BulkLoadRecordWriter) extends Writer {

  def write(doc: Document, key: ByteBuffer) = {
    table.write(DBValueBuilderImpl.KeyValueBuilder.initFrom(key), doc)
  }

  def write(doc: Document, key: String) = {
    table.write(DBValueBuilderImpl.KeyValueBuilder.initFrom(key),doc)
  }

  def write(doc: Document, key: org.ojai.Value) = {
    table.write(key, doc)
  }

  def close() = {
    table.close(null)
  }
}