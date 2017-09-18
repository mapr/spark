/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import com.mapr.db.spark.utils.MapRSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe._

case class SparkSessionFunctions(@transient sparkSession: SparkSession) extends Serializable {

  def loadFromMapRDB[T <: Product : TypeTag](tableName: String,
                                             schema : StructType = null,
                                             sampleSize: Double = GenerateSchema.SAMPLE_SIZE): DataFrame = {
    MapRSpark.builder()
             .sparkSession(sparkSession)
             .configuration()
             .sparkContext(sparkSession.sparkContext)
             .setTable(tableName).build().toDF[T](schema, sampleSize)
  }
}