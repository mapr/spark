/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.streaming

import com.mapr.db.spark.streaming.sink.MapRDBSink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode

class DefaultSource extends StreamSinkProvider with DataSourceRegister with Logging {

  override def shortName(): String = "maprdb"

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    new MapRDBSink(parameters)
  }

}
