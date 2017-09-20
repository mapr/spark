/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
