/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.writers

import java.nio.ByteBuffer

import com.mapr.db.mapreduce.BulkLoadRecordWriter
import com.mapr.db.spark.configuration.SerializableConfiguration
import com.mapr.db.spark.dbclient.DBClient
import org.apache.hadoop.fs.Path
import org.ojai.Document

private[spark] trait Writer extends Serializable {

  def write(doc: Document, key: ByteBuffer)

  def write(doc: Document, key: String)

  def write(doc: Document, key: org.ojai.Value)

  def close()
}

private[spark] object Writer {
  def initialize(tableName: String,
                 serializableConfiguration: SerializableConfiguration,
                 bulkInsert: Boolean, insertOrReplace : Boolean, bufferWrites: Boolean): Writer = {

    if (!bulkInsert) {
      if (insertOrReplace) {
        TableInsertOrReplaceWriter(DBClient().getTable(tableName, bufferWrites))
      } else TableInsertWriter(DBClient().getTable(tableName, bufferWrites))
    }
    else BulkTableWriter(
      new BulkLoadRecordWriter(serializableConfiguration.value, new Path(tableName)))
  }
}
