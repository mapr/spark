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

object MaprDBJsonConnectorWordCount {

  def main(args: Array[String]): Unit = {

    parseArgs(args)

    val pathToFileWithData = args(0)
    val tableName = args(1)
    val tableNameWithResult = args(2)

    val spark = SparkSession
      .builder()
      .appName("OJAI MaprDB connector wordcount example")
      .getOrCreate()

    import spark.implicits._

    val wordDF = spark.sparkContext.textFile(pathToFileWithData)
      .map(line => {
        val wordWithId = line.split(" ")
        Word(wordWithId(0), wordWithId.drop(1).mkString(" "))
      }).toDF

    wordDF.saveToMapRDB(tableName, createTable = true)

    val dfWithDataFromMaprDB = spark.loadFromMapRDB(tableName)
      .flatMap(line => line.getAs[String](1).split(" "))
      .groupBy("value")
      .count()

    println("Dataset with counted words:")
    dfWithDataFromMaprDB.show()

    dfWithDataFromMaprDB.withColumn("_id", $"value")
      .saveToMapRDB(tableNameWithResult, createTable = true)
    println("Dataset with counted words was saved into the MaprDB table.")

    spark.stop()
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != 3) {
      printUsage()
      System.exit(1)
    }
  }

  private def printUsage(): Unit = {
    val usage =
      """OJAI MaprDB connector wordcount example
        |Usage:
        |1) path to the file with data (words.txt can be used for the test)
        |   by default Spark will search file in maprfs. If you want to use local file
        |   you need to add "file:///" before a path to a file;
        |2) name of the MaprDB table where data from file will be saved;
        |3) name of the MaprDB table where result will be saved;
        |""".stripMargin

    println(usage)
  }

  private case class Word(_id: String, words: String)

}
