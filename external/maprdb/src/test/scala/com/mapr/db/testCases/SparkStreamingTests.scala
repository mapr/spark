/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.testCases

import com.mapr.db.spark._
import com.mapr.db.spark.dbclient.DBClient
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.mapr.db.spark.streaming._
import org.apache.spark.sql.SparkSession

object SparkStreamingTests {
  lazy val conf = new SparkConf()
    .setAppName("SparkStreamingTests")
    .set ("spark.executor.memory", "1g")
    .set ("spark.driver.memory", "1g")

  def functionToCreateContext(spark: SparkContext)(): StreamingContext = {
    val ssc = new StreamingContext(spark, Seconds(10))
    ssc
  }
  lazy val ssc = StreamingContext.getOrCreate("/tmp/",
    functionToCreateContext(SparkSession.builder().appName("SparkStreaming").config(conf).getOrCreate().sparkContext))

  val tableName = "/tmp/SparkSqlOjaiConnectorStreamingTesting"

  def main (args: Array[String] ): Unit = {
    MapRDBSparkTests.tableInitialization (ssc.sparkContext,tableName)
    SparkStreamingTests.runTests(ssc.sparkContext)
  }

  def runTests(sparkSession: SparkContext): Unit = {
    testSavingDStreamToMapRDBTable(ssc, tableName)
  }

  def testSavingDStreamToMapRDBTable(spark: StreamingContext, tableName: String): Boolean = {
    if (DBClient().tableExists(tableName+"output")) DBClient().deleteTable(tableName+"output")
    val rdd = spark.sparkContext.loadFromMapRDB(tableName)
    val dstream = new ConstantInputDStream(spark, rdd)
    dstream.saveToMapRDB(tableName+"output", createTable = true)

    spark.start()
    Thread.sleep(10000)
    val collection = ssc.sparkContext.loadFromMapRDB(tableName+"output").collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}"))) {
      println("testSavingDStreamToMapRDBTable succeeded")
      return true
    } else {
      println("testSavingDStreamToMapRDBTable failed")
      collection.foreach(doc => println(doc.asJsonString()))
      return false
    }
  }
}

object SparkStreamingTestsWithKryo {
  val tableName = "/tmp/SparkSqlOjaiConnectorStreamingTestingKryo"
  lazy val conf = new SparkConf()
    .setAppName("SparkStreamingTestsWithKryo")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")

  def functionToCreateContext(spark: SparkContext)(): StreamingContext = {
    val ssc = new StreamingContext(spark, Seconds(10))
    ssc
  }
  lazy val ssc = StreamingContext.getOrCreate("/tmp/",
    functionToCreateContext(SparkSession.builder().appName("SparkStreaming").config(conf).getOrCreate().sparkContext))

  def main (args: Array[String] ): Unit = {
    MapRDBSparkTests.tableInitialization (ssc.sparkContext,tableName)
    SparkStreamingTests.runTests(ssc.sparkContext)
  }
}
