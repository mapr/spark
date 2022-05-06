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

// scalastyle:off println
package org.apache.spark.examples.maprdbconnector

import org.apache.spark.sql.SparkSession

import com.mapr.db.spark.sql._

object MaprDBJsonConnectorDeleteFromTable {

  def main(args: Array[String]): Unit = {
    parseArgs(args)

    val tableName = args(0)
    val tableNameWithDeletable = args(1)

    val spark = SparkSession
      .builder()
      .appName("OJAI MaprDB connector wordcount example")
      .getOrCreate()

    val dfContentFromMaprDB = spark.loadFromMapRDB(tableName)

    println("Dataset before delete:")
    dfContentFromMaprDB.show()

    val dfDeleteFromMaprDB = spark.loadFromMapRDB(tableNameWithDeletable)

    println("Dataset what to delete:")
    dfDeleteFromMaprDB.show()

    dfDeleteFromMaprDB.deleteFromMapRDB(tableName)

    println("Dataset after delete:")
    dfContentFromMaprDB.show()

    spark.stop()
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != 2) {
      printUsage()
      System.exit(1)
    }
  }

  private def printUsage(): Unit = {
    val usage =
      """OJAI MaprDB connector delete example
        |Usage:
        |1) name of the MaprDB table with some content;
        |2) name of the MaprDB table with content that
        |   should be deleted from the first table;
        |""".stripMargin

    println(usage)
  }
}
