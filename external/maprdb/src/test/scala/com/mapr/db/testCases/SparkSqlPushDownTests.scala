/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.testCases

import com.mapr.db.spark.field
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import com.mapr.db.spark._
import com.mapr.db.spark.sql._
import org.apache.spark.sql.functions._


object SparkSqlPushDownTests {
  lazy val conf = new SparkConf()
                  .setAppName("SparkSqlFilterTests")
                  .set ("spark.executor.memory", "1g")
                  .set ("spark.driver.memory", "1g")

  lazy val spark = SparkSession.builder ().appName ("SparkSqlFilterTests").config (conf).getOrCreate ()

  val tableName = "/tmp/SparkSqlOjaiConnectorFilterTesting"

  def main (args: Array[String] ): Unit = {
    MapRDBSparkTests.tableInitialization (spark.sparkContext,tableName)
    SparkSqlPushDownTests.runTests(spark)
  }

  def testFilterPushDownOnIdColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"_id" === "rsmith").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("EqualTo(_id,rsmith)")) {
      println("testFilterPushDownOnIdColumn succeeded")
      return true
    }
    else {
      println("testFilterPushDownOnIdColumn failed")
      return false
    }
  }


  def testFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name" === "Andrew").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("EqualTo(first_name,Andrew)")) {
      println("testFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println("testFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testFilterPushDownOnMapColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"address.city" === "San Jose").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (!(pushDownHappened && subStr.contains("EqualTo(address.city,San Jose)"))) {
      if (pushDownHappened == false) println("Filter is not pushed down")
      println("testFilterPushDownOnMapColumn succeeded")
      return true
    }
    else {
      println("testFilterPushDownOnMapColumn failed")
      return false
    }
  }

  def testFilterPushDownOnArrayColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"interests"(0) === "San Jose").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (!(pushDownHappened && subStr.contains("EqualTo(interests(0),San Jose)"))) {
      if (pushDownHappened == false) println("Filter is not pushed down")
      println("testFilterPushDownOnArrayColumn succeeded")
      return true
    }
    else {
      println("testFilterPushDownOnArrayColumn failed")
      return false
    }
  }

  def testGTFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name" > "Andrew").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("GreaterThan(first_name,Andrew)")) {
      println("testGTFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      println("testGTFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testLTFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name" < "Andrew").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("LessThan(first_name,Andrew)")) {
      println("testLTFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      println("testLTFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testLTEFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name" <= "Andrew").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("LessThanOrEqual(first_name,Andrew)")) {
      println("testLTEFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      println("testLTEFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testGTEFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name" >= "Andrew").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("GreaterThanOrEqual(first_name,Andrew)")) {
      println("testGTEFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      println("testGTEFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testComplexOrFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name" >= "Andrew" or $"first_name" <= "Andrew").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("Or(GreaterThanOrEqual(first_name,Andrew),LessThanOrEqual(first_name,Andrew))")) {
      println("testComplexOrFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      println("testComplexOrFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testComplexAndFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name" >= "Andrew" and $"last_name" === "Lehmann").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("GreaterThanOrEqual(first_name,Andrew)") && subStr.contains("EqualTo")) {
      println("testComplexAndFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      println(subStr.substring(70), subStr.length)
      println("testComplexAndFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testStartsWithPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name".startsWith("And")).queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("StringStartsWith(first_name,And)")) {
      println("testStartsWithPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      //println(subStr.substring(70), subStr.length)
      println("testStartsWithPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testEndsWithPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name".endsWith("rew")).queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("StringEndsWith(first_name,rew)")) {
      println("testEndsWithPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      //println(subStr.substring(70), subStr.length)
      println("testEndsWithPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testContainsPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name".contains("dre")).queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("StringContains(first_name,dre)")) {
      println("testContainsPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      //println(subStr.substring(70), subStr.length)
      println("testContainsPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testINPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter($"first_name".isin("Andrew")).queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("In(first_name, [Andrew]")) {
      println("testINPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      //println(subStr.substring(70), subStr.length)
      println("testINPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testComplexNotOrFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter(not($"first_name" >= "Andrew" or $"first_name" <= "Andrew")).queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("LessThan(first_name,Andrew)") && subStr.contains("GreaterThan(first_name,Andrew)")) {
      println("testComplexNotOrFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      println("testComplexNotOrFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testComplexNotAndFilterPushDownOnNonIDColumn(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).filter(not($"first_name" >= "Andrew" and $"last_name" === "Lehmann")).queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("Or(LessThan(first_name,Andrew),Not(EqualTo(last_name,Lehmann)))")) {
      println("testComplexNotAndFilterPushDownOnNonIDColumn succeeded")
      return true
    }
    else {
      println(subStr)
      println(subStr.substring(70), subStr.length)
      println("testComplexNotAndFilterPushDownOnNonIDColumn failed")
      return false
    }
  }

  def testProjectionPushDown(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).select("first_name","last_name","_id").queryExecution.sparkPlan.toString()
    val subStr = getPushedColumns(executionPlan)
    if (subStr.contains("first_name") && subStr.contains("last_name") && subStr.contains("_id") && subStr.split(",").length == 3 ) {
      println("testProjectionPushDown succeeded")
      return true
    }
    else {
      println(subStr)
      subStr.split(",").foreach(println)
      println("testProjectionPushDown failed")
      return false
    }
  }

  def testProjectionPushDownNestedFields(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.loadFromMapRDB(tableName).select("first_name","last_name","_id","address").queryExecution.executedPlan.toString()
    val subStr = getPushedColumns(executionPlan)
    if (subStr.contains("first_name") && subStr.contains("last_name") && subStr.contains("_id") && subStr.contains("address") && !subStr.contains("city") && subStr.split(",").length == 4 ) {
      println("testProjectionPushDownNestedFields succeeded")
      return true
    }
    else {
      println(subStr)
      subStr.split(",").foreach(println)
      println("testProjectionPushDownNestedFields failed")
      return false
    }
  }

  def testProjectionPDOnDFWithRDDSelection(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.sparkContext.loadFromMapRDB(tableName).select("first_name","last_name","_id","address").toDF.select("first_name","last_name").queryExecution.executedPlan.toString()
    val subStr = getPushedColumns(executionPlan)
    if (subStr.contains("first_name") && subStr.contains("last_name") && !subStr.contains("_id") && !subStr.contains("address") && !subStr.contains("city") && subStr.split(",").length == 2 ) {
      println("testProjectionOnDFWithRDDSelection succeeded")
      return true
    }
    else {
      println(subStr)
      subStr.split(",").foreach(println)
      println("testProjectionOnDFWithRDDSelection failed")
      return false
    }
  }

  def testProjectionPDOnDFWithRDDSelectionErrorCondition(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    var executionPlan: String = null
    try {
      executionPlan = spark.sparkContext.loadFromMapRDB(tableName).select("first_name", "last_name", "_id", "address").toDF.select("first_name", "last_name", "dob").queryExecution.executedPlan.toString()
    } catch {
      case ex: AnalysisException => {
        println("testProjectionPDOnDFWithRDDSelectionErrorCondition succeeded")
        return true
      }
    }
    val subStr = getPushedColumns(executionPlan)
    if (subStr.contains("first_name") && subStr.contains("last_name") && !subStr.contains("_id") && !subStr.contains("address") && !subStr.contains("city") && subStr.split(",").length == 2 ) {
      println("testProjectionPDOnDFWithRDDSelectionErrorCondition succeeded")
      return true
    }
    else {
      println(subStr)
      subStr.split(",").foreach(println)
      println("testProjectionPDOnDFWithRDDSelectionErrorCondition failed")
      return false
    }
  }

  def testFilterPDOnDFWithRDDFilter(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val executionPlan = spark.sparkContext.loadFromMapRDB(tableName).where(field("first_name") <= "Andrew").toDF.filter($"first_name" >= "Andrew").queryExecution.sparkPlan.toString()
    val pushDownHappened = executionPlan.contains("PushedFilters:")
    val subStr = if (pushDownHappened) getPrunedFilters(executionPlan) else ""
    if (pushDownHappened && subStr.contains("GreaterThanOrEqual(first_name,Andrew)")) {
      println("testFilterPDOnDFWithRDDFilter succeeded")
      return true
    }
    else {
      println(subStr)
      println("testFilterPDOnDFWithRDDFilter failed")
      return false
    }
  }

  def runTests(sparkSession: SparkSession): Unit = {
    testFilterPushDownOnIdColumn(spark, tableName)
    testFilterPushDownOnNonIDColumn(spark, tableName)
    testFilterPushDownOnMapColumn(spark, tableName)
    testFilterPushDownOnArrayColumn(spark, tableName)
    testLTFilterPushDownOnNonIDColumn(spark, tableName)
    testLTEFilterPushDownOnNonIDColumn(spark, tableName)
    testGTFilterPushDownOnNonIDColumn(spark, tableName)
    testGTEFilterPushDownOnNonIDColumn(spark, tableName)
    testComplexOrFilterPushDownOnNonIDColumn(spark, tableName)
    testComplexAndFilterPushDownOnNonIDColumn(spark, tableName)
    testStartsWithPushDownOnNonIDColumn(spark, tableName)
    testEndsWithPushDownOnNonIDColumn(spark, tableName)
    testContainsPushDownOnNonIDColumn(spark, tableName)
    testINPushDownOnNonIDColumn(spark, tableName)
    testProjectionPushDown(spark, tableName)
    testProjectionPushDownNestedFields(spark, tableName)
    testProjectionPDOnDFWithRDDSelection(spark, tableName)
    testFilterPDOnDFWithRDDFilter(spark, tableName)
    testProjectionPDOnDFWithRDDSelectionErrorCondition(spark, tableName)
  }

  def getPrunedFilters(executionPlan: String): String = {
    val startIndex: Int = executionPlan.indexOf("PushedFilters:")
    val endIndex: Int = executionPlan.indexOf("ReadSchema")
    return executionPlan.substring(startIndex, endIndex)
  }

  def getPushedColumns(executionPlan: String): String = {
    val startIndex: Int = executionPlan.indexOf("MapRDBBaseRDD")
    val colStartIndex: Int = executionPlan.substring(startIndex).indexOf('[') + startIndex
    val colEndIndex: Int = if (executionPlan.substring(startIndex).indexOf(']') != -1) executionPlan.substring(startIndex).indexOf(']')  + startIndex
                           else executionPlan.substring(startIndex).indexOf("ReadSchema") + startIndex
    return executionPlan.substring(colStartIndex, colEndIndex)
  }
}

object SparkSqlPushDownTestsWithKryo {
  val tableName = "/tmp/SparkSqlOjaiConnectorFilterTesting"
  lazy val conf = new SparkConf()
    .setAppName("SparkSqlFilterTestsWithKryo")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")

  lazy val spark = SparkSession.builder().appName("SparkSqlFilterTestsWithKryo").config(conf).getOrCreate()


  def main(args: Array[String]): Unit = {
    MapRDBSparkTests.tableInitialization(spark.sparkContext,tableName)
    SparkSqlPushDownTests.runTests(spark)
  }
}