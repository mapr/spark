package com.mapr.db.spark.streaming

object MapRDBSourceConfig {

  val Format: String = classOf[DefaultSource].getPackage.getName
  val TablePathOption: String = "tablePath"
  val IdFieldPathOption: String = "idFieldPath"
  val CreateTableOption: String = "createTable"
  val BulkModeOption: String = "bulkMode"
  val SampleSizeOption: String = "sampleSize"

}
