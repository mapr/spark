/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD.api.java;

import scala.reflect.ClassTag

import com.mapr.db.spark.RDD.MapRDBBaseRDD
import org.ojai.store.QueryCondition

import org.apache.spark.api.java.JavaRDD


case class MapRDBJavaRDD[R: ClassTag](override val rdd: MapRDBBaseRDD[R])
    extends JavaRDD[R](rdd) {

  def where(condition: QueryCondition): MapRDBJavaRDD[R] = {
    MapRDBJavaRDD(rdd.where(condition))
  }

  @annotation.varargs
  def select(projectedFields: String*): MapRDBJavaRDD[R] = {
    MapRDBJavaRDD(rdd.select[String](projectedFields: _*))
  }
}
