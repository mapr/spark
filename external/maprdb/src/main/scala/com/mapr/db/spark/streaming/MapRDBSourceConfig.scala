/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.streaming

object MapRDBSourceConfig {

  val Format: String = classOf[DefaultSource].getPackage.getName
  val TablePathOption: String = "tablePath"
  val BufferWrites: String = "bufferWrites"
  val IdFieldPathOption: String = "idFieldPath"
  val CreateTableOption: String = "createTable"
  val BulkModeOption: String = "bulkMode"
  val SampleSizeOption: String = "sampleSize"

}
