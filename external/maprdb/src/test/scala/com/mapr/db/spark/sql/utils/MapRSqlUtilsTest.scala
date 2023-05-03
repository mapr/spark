/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql.utils

import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.dbclient.DBClient
import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.types.{StringType, StructField, StructType}

class MapRSqlUtilsTest extends SparkFunSuite {

  test("MapR [SPARK-297] Empty value converted as non-null") {

    val schema = StructType(
      List(
        StructField("non_empty_string", StringType, true),
        StructField("empty_string", StringType, true),
        StructField("null_string", StringType, true)
      )
    )

    val document = MapRDBSpark.newDocument(DBClient().newDocument())
    document.set("non_empty_string", "non_empty_value")
    document.set("empty_string", "")
    document.setNull("null_string")

    val row = MapRSqlUtils.documentToRow(document, schema)

    assert(row != null, "Result row can not be null")
    assert("non_empty_value" == row.getAs[String]("non_empty_string"))
    assert("" == row.getAs[String]("empty_string"))
    assert(null == row.getAs[String]("null_string"))
  }
}
