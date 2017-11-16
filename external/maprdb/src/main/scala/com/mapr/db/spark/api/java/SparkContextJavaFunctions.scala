/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.db.spark.api.java

import com.mapr.db.spark.RDD.RDDTYPE
import com.mapr.db.spark.RDD.api.java.MapRDBJavaRDD
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.utils.MapRSpark
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext

class SparkContextJavaFunctions(val sparkContext: SparkContext) {

  def loadFromMapRDB(tableName: String): MapRDBJavaRDD[OJAIDocument] = {
    val rdd = MapRSpark
      .builder()
      .sparkContext(sparkContext)
      .configuration(new Configuration)
      .setTable(tableName)
      .build()
      .toJavaRDD(classOf[OJAIDocument])

    MapRDBJavaRDD(rdd)
  }

  def loadFromMapRDB[D <: java.lang.Object](
      tableName: String,
      clazz: Class[D]): MapRDBJavaRDD[D] = {

    import scala.reflect._
    implicit val ct: ClassTag[D] = ClassTag(clazz)
    implicit val rddType: RDDTYPE[D] = RDDTYPE.overrideJavaDefaultType[D]

    val rdd = MapRSpark
      .builder()
      .sparkContext(sparkContext)
      .configuration(new Configuration)
      .setTable(tableName)
      .build()
      .toJavaRDD(clazz)

    MapRDBJavaRDD(rdd)
  }

}
