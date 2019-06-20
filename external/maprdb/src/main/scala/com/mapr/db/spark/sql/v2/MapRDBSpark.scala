package com.mapr.db.spark.sql.v2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

package object MapRDBSpark {

  implicit class SessionOps(sparkSession: SparkSession) {

    /**
      * Entry point to the DataSource API
      *
      * @param path   MapR Table path
      * @param schema Schema to be read
      * @param many   How many readers per tablet.
      * @return
      */
    def loadFromMapRDB(path: String, schema: StructType, many: Int = 1): DataFrame = {
      sparkSession
        .read
        .format("com.mapr.db.spark.sql.v2.Reader")
        .schema(schema)
        .option("readers", many)
        .load(path)
    }
  }

}
