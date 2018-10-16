/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.writers

import org.ojai.Document
import java.nio.ByteBuffer
import org.apache.hadoop.fs.Path
import com.mapr.db.mapreduce.BulkLoadRecordWriter
import com.mapr.db.spark.configuration.SerializableConfiguration
import com.mapr.db.spark.dbclient.DBClient

private[spark] trait Writer extends Serializable {

  def write(doc: Document, key: ByteBuffer)

  def write(doc: Document, key: String)

  def write(doc: Document, key: org.ojai.Value)

  def close()
}

private[spark] object Writer {
  def initialize(tableName: String,
                 serializableConfiguration: SerializableConfiguration,
                 bulkInsert: Boolean, insertOrReplace : Boolean): Writer = {

    if (!bulkInsert) {
      if (insertOrReplace) {
        TableInsertOrReplaceWriter(DBClient().getTable(tableName))
      } else TableInsertWriter(DBClient().getTable(tableName))
    }
    else BulkTableWriter(
      new BulkLoadRecordWriter(serializableConfiguration.value, new Path(tableName)))
  }
}