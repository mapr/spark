/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.testCases

import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.apache.spark.{SparkConf, SparkContext}
import org.ojai.types.ODate
import com.mapr.db.spark.field
import com.mapr.db.spark._
import com.mapr.db.spark.types.DBBinaryValue

import scala.language.implicitConversions
import com.mapr.db.MapRDB
import org.apache.spark.rdd.RDD

object LoadAndSaveTests {
  lazy val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("simpletest")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
  val tableName = "/tmp/user_profiles_load_and_save_read"
  val saveToTable = "/tmp/user_profiles_load_and_save_write"

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(conf)
    tableInitialization(sc, tableName)
    runTests(sc, false)
  }

  def tableInitialization(sparkSession: SparkContext, tableName: String): Unit = {
    val jsonText =
      """
        |{
        |   "_id":"rsmith",
        |   "address":{
        |      "city":"San Francisco",
        |      "line":"100 Main Street",
        |      "zip":94105
        |   },
        |   "dob":"1982-02-03",
        |   "first_name":"Robert",
        |   "interests":[
        |      "electronics",
        |      "music",
        |      "sports"
        |   ],
        |   "last_name":"Smith"
        |},
        |{
        |   "_id":"mdupont",
        |   "address":{
        |      "city":"San Jose",
        |      "line":"1223 Broadway",
        |      "zip":95109
        |   },
        |   "dob":"1982-02-03",
        |   "first_name":"Maxime",
        |   "interests":[
        |      "sports",
        |      "movies",
        |      "electronics"
        |   ],
        |   "last_name":"Dupont"
        |},
        |{
        |   "_id":"jdoe",
        |   "dob":"1970-06-23",
        |   "first_name":"John",
        |   "last_name":"Doe"
        |},
        |{
        |   "_id":"dsimon",
        |   "dob":"1980-10-13",
        |   "first_name":"David",
        |   "last_name":"Simon"
        |},
        |{
        |   "_id":"alehmann",
        |   "dob":"1980-10-13",
        |   "first_name":"Andrew",
        |   "interests":[
        |      "html",
        |      "css",
        |      "js"
        |   ],
        |   "last_name":"Lehmann"
        |}
      """.stripMargin

    if (MapRDB.tableExists(tableName)) {
      MapRDB.deleteTable(tableName)
    }

    val docs = scala.collection.immutable.List(jsonText)
    val docsrdd = sparkSession.parallelize(docs)
    val ojairdd = docsrdd
      .map(doc => MapRDBSpark.newDocument(doc))
      .map(doc => {
        doc.dob = Option(doc.dob) match {
          case Some(a) => ODate.parse(doc.dob[String])
          case None => null
        }
        doc
      })
    ojairdd.saveToMapRDB(tableName, createTable = true)
  }

  def runTests(sparkSession: SparkContext, kryoOn: Boolean): Unit = {
    testingSimpleLoadTable(sparkSession, tableName)
    testingLoadTableWithSpecificColumns(sparkSession, tableName)
    testingLoadTableWithWhereEQCondition(sparkSession, tableName)
    testingLoadTableWithWhereEQAndSelectClause(sparkSession, tableName)
    testingLoadTableWithWhereEQConditionAndSave(sparkSession, tableName)
    testingSimpleSaveTable(sparkSession, tableName, saveToTable)
    testingIDwithSaveToMapRDB(sparkSession, tableName, saveToTable + "id")
    testingBulkSaveMode(sparkSession, tableName, saveToTable)
    testingBulkSaveWithoutBulkModeSetInTable(sparkSession, tableName, saveToTable)
    testingBulkSaveModeBeanClass(sparkSession, tableName, saveToTable)
    testingSplitPartitioner(sparkSession, tableName, saveToTable)
    testingSplitPartitionerWithBinaryData(sparkSession, tableName, saveToTable)
    if (kryoOn) testingSplitPartitionerWithByteBufferData(sparkSession, tableName, saveToTable)
  }

  def testingSimpleLoadTable(sc: SparkContext, tableName: String): Boolean = {
    val jsonTxt =
      """
        |{
        |   "_id":"rsmith",
        |   "address":{
        |      "city":"San Francisco",
        |      "line":"100 Main Street",
        |      "zip":94105
        |   },
        |   "dob":"1982-02-03",
        |   "first_name":"Robert",
        |   "interests":[
        |      "electronics",
        |      "music",
        |      "sports"
        |   ],
        |   "last_name":"Smith"
        |},
        |{
        |   "_id":"mdupont",
        |   "address":{
        |      "city":"San Jose",
        |      "line":"1223 Broadway",
        |      "zip":95109
        |   },
        |   "dob":"1982-02-03",
        |   "first_name":"Maxime",
        |   "interests":[
        |      "sports",
        |      "movies",
        |      "electronics"
        |   ],
        |   "last_name":"Dupont"
        |},
        |{
        |   "_id":"jdoe",
        |   "dob":"1970-06-23",
        |   "first_name":"John",
        |   "last_name":"Doe"
        |},
        |{
        |   "_id":"dsimon",
        |   "dob":"1980-10-13",
        |   "first_name":"David",
        |   "last_name":"Simon"
        |},
        |{
        |   "_id":"alehmann",
        |   "dob":"1980-10-13",
        |   "first_name":"Andrew",
        |   "interests":[
        |      "html",
        |      "css",
        |      "js"
        |   ],
        |   "last_name":"Lehmann"
        |}
      """.stripMargin


    val cust_profiles = sc.loadFromMapRDB(tableName).collect()
    if (cust_profiles
      .map(a => a.asJsonString())
      .toSet
      .equals(Set(jsonTxt))) {

      println("testingSimpleLoadTable succeeded")
      true
    } else {
      println("testingSimpleLoadTable failed")
      false
    }
  }

  def testingLoadTableWithSpecificColumns(sc: SparkContext,
                                          tableName: String) = {
    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .select("first_name", "address", "interests")
      .collect

    if (cust_profiles
      .map(a => a.asJsonString())
      .toSet
      .equals(Set(
        "{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"first_name\":\"Robert\"," +
          "\"interests\":[\"electronics\",\"music\",\"sports\"]}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"]}",
        "{\"_id\":\"jdoe\",\"first_name\":\"John\"}",
        "{\"_id\":\"dsimon\",\"first_name\":\"David\"}",
        "{\"_id\":\"alehmann\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"]}"
      ))) {
      println("testingLoadTableWithSpecificColumns succeeded")
      true
    } else {
      println("testingLoadTableWithSpecificColumns failed")
      false
    }
  }

  def testingLoadTableWithWhereEQCondition(sc: SparkContext,
                                           tableName: String) = {
    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .where(field("address.city") === "San Jose")
      .collect

    if (cust_profiles
      .map(a => a.asJsonString())
      .toSet
      .equals(Set(
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}"))) {
      println("testingLoadTableWithWhereEQCondition succeeded")
      true
    } else {
      println("testingLoadTableWithWhereEQCondition failed")
      false
    }
  }

  def testingLoadTableWithWhereEQConditionAndSave(sc: SparkContext,
                                                  tableName: String) = {
    if (MapRDB.tableExists(tableName + "save"))
      MapRDB.deleteTable(tableName + "save")
    val cust_profiles =
      sc.loadFromMapRDB(tableName).where(field("address.city") === "San Jose")
    cust_profiles.saveToMapRDB(tableName + "save", createTable = true)
    val cust_profiles1 = sc.loadFromMapRDB(tableName + "save").collect
    if (cust_profiles1
      .map(a => a.asJsonString())
      .toSet
      .equals(Set(
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}"))) {
      println("testingLoadTableWithWhereEQConditionAndSave succeeded")
      true
    } else {
      println("testingLoadTableWithWhereEQConditionAndSave failed")
      false
    }
  }

  def testingLoadTableWithWhereEQAndSelectClause(sc: SparkContext,
                                                 tableName: String) = {
    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .where(field("address.city") === "San Jose")
      .select("address", "first_name", "interests")
      .collect

    if (cust_profiles
      .map(a => a.asJsonString())
      .toSet
      .equals(Set(
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"]}"))) {
      println("testingLoadTableWithWhereEQAndSelectClause succeeded")
      true
    } else {
      println("testingLoadTableWithWhereEQAndSelectClause failed")
      false
    }
  }

  def testingSimpleSaveTable(sc: SparkContext,
                             tableName: String,
                             saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable))
      MapRDB.deleteTable(saveToTable)
    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .map(a => a.setId(a.first_name[String]))
      .keyBy(a => a.first_name[String])
      .partitionBy(MapRDBSpark.newPartitioner[String](tableName))
    cust_profiles.saveToMapRDB(saveToTable, createTable = true)
    val saved_profiles = sc.loadFromMapRDB(saveToTable).collect
    if (saved_profiles
      .map(a => (a.getIdString, a.asJsonString))
      .toSet
      .equals(Set(
        ("Robert",
          "{\"_id\":\"Robert\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}"),
        ("Maxime",
          "{\"_id\":\"Maxime\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}"),
        ("John",
          "{\"_id\":\"John\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}"),
        ("David",
          "{\"_id\":\"David\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}"),
        ("Andrew", "{\"_id\":\"Andrew\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
      ))) {
      println("testingSimpleSaveTable succeeded")
      true
    } else {
      println("testingSimpleSaveTable failed")
      saved_profiles
        .map(a => (a.getIdString, a.asJsonString))
        .foreach(println(_))
      false
    }
  }

  def testingIDwithSaveToMapRDB(sc: SparkContext,
                                tableName: String,
                                saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable))
      MapRDB.deleteTable(saveToTable)
    val cust_profiles = sc.loadFromMapRDB(tableName)
    cust_profiles.saveToMapRDB(saveToTable,
      idFieldPath = "first_name",
      createTable = true)
    val saved_profiles = sc.loadFromMapRDB(saveToTable).collect
    if (saved_profiles
      .map(a => (a.getIdString, a.asJsonString))
      .toSet
      .equals(Set(
        ("Robert",
          "{\"_id\":\"Robert\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}"),
        ("Maxime",
          "{\"_id\":\"Maxime\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}"),
        ("John",
          "{\"_id\":\"John\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}"),
        ("David",
          "{\"_id\":\"David\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}"),
        ("Andrew", "{\"_id\":\"Andrew\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
      ))) {
      println("testingIDwithSaveToMapRDB succeeded")
      true
    } else {
      println("testingIDwithSaveToMapRDB failed")
      false
    }
  }

  def testingBulkSaveMode(sc: SparkContext,
                          tableName: String,
                          saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable + "bulk"))
      MapRDB.deleteTable(saveToTable + "bulk")
    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .keyBy(a => a.first_name[String])
      .repartitionAndSortWithinPartitions(
        MapRDBSpark.newPartitioner[String](tableName))
    cust_profiles.saveToMapRDB(saveToTable + "bulk",
      bulkInsert = true,
      createTable = true)
    val saved_profiles = sc.loadFromMapRDB(saveToTable + "bulk").collect
    if (saved_profiles
      .map(a => (a.getIdString, a.asJsonString))
      .toSet
      .equals(Set(
        ("Robert",
          "{\"_id\":\"Robert\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}"),
        ("Maxime",
          "{\"_id\":\"Maxime\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}"),
        ("John",
          "{\"_id\":\"John\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}"),
        ("David",
          "{\"_id\":\"David\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}"),
        ("Andrew", "{\"_id\":\"Andrew\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
      ))) {
      println("testingBulkSaveMode succeeded")
      true
    } else {
      println("testingBulkSaveMode failed")
      saved_profiles
        .map(a => (a.getIdString, a.asJsonString))
        .foreach(println(_))
      false
    }
  }

  def testingBulkSaveWithoutBulkModeSetInTable(sc: SparkContext,
                                               tableName: String,
                                               saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable + "bulkmodeswitch"))
      MapRDB.deleteTable(saveToTable + "bulkmodeswitch")
    val tabDesc = MapRDB.newTableDescriptor()
    tabDesc.setAutoSplit(true)
    tabDesc.setPath(saveToTable + "bulkmodeswitch")
    tabDesc.setBulkLoad(false)
    MapRDB.newAdmin().createTable(tabDesc)
    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .keyBy(a => a.first_name[String])
      .repartitionAndSortWithinPartitions(
        MapRDBSpark.newPartitioner[String](tableName))
    cust_profiles.saveToMapRDB(saveToTable + "bulkmodeswitch",
      bulkInsert = true,
      createTable = false)
    val saved_profiles =
      sc.loadFromMapRDB(saveToTable + "bulkmodeswitch").collect
    if (saved_profiles
      .map(a => (a.getIdString, a.asJsonString))
      .toSet
      .equals(Set(
        ("Robert",
          "{\"_id\":\"Robert\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}"),
        ("Maxime",
          "{\"_id\":\"Maxime\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}"),
        ("John",
          "{\"_id\":\"John\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}"),
        ("David",
          "{\"_id\":\"David\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}"),
        ("Andrew", "{\"_id\":\"Andrew\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
      ))) {
      println("testingBulkSaveMode succeeded")
      true
    } else {
      println("testingBulkSaveMode failed")
      saved_profiles
        .map(a => (a.getIdString, a.asJsonString))
        .foreach(println(_))
      false
    }
  }

  def testingBulkSaveModeBeanClass(sc: SparkContext,
                                   tableName: String,
                                   saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable + "bulkbean"))
      MapRDB.deleteTable(saveToTable + "bulkbean")
    val cust_profiles = sc
      .loadFromMapRDB[BeanTest.User](tableName)
      .keyBy(a => a.firstName)
      .repartitionAndSortWithinPartitions(
        MapRDBSpark.newPartitioner[String](tableName))
    cust_profiles.saveToMapRDB(saveToTable + "bulkbean",
      bulkInsert = true,
      createTable = true)
    val saved_profiles = sc.loadFromMapRDB(saveToTable + "bulkbean").collect
    if (saved_profiles
      .map(a => (a.getIdString, a.asJsonString()))
      .toSet
      .sameElements(Set(
        ("Robert",
          "{\"_id\":\"Robert\",\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}"),
        ("Maxime",
          "{\"_id\":\"Maxime\",\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}"),
        ("John",
          "{\"_id\":\"John\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"interests\":null,\"last_name\":\"Doe\"}"),
        ("David",
          "{\"_id\":\"David\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"interests\":null,\"last_name\":\"Simon\"}"),
        ("Andrew", "{\"_id\":\"Andrew\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
      ))) {
      println("testingBulkSaveModeBeanClass succeded")
      true
    } else {
      println("testingBulkSaveModeBeanClass failed")
      saved_profiles
        .map(a => (a.getIdString, a.asJsonString))
        .foreach(println(_))
      false
    }
  }

  def testingSplitPartitioner(sc: SparkContext,
                              tableName: String,
                              saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable + "splitpartitioner"))
      MapRDB.deleteTable(saveToTable + "splitpartitioner")
    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .keyBy(a => a.`first_name`[String])
      .repartitionAndSortWithinPartitions(MapRDBSpark.newPartitioner[String](
        Seq("bolo", "chalo", "hello", "zebra")))
    cust_profiles.saveToMapRDB(saveToTable + "splitpartitioner",
      bulkInsert = true,
      createTable = true)
    val saved_profiles =
      sc.loadFromMapRDB(saveToTable + "splitpartitioner").collect
    if (saved_profiles
      .map(a => (a.getIdString, a.asJsonString))
      .toSet
      .equals(Set(
        ("Robert",
          "{\"_id\":\"Robert\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}"),
        ("Maxime",
          "{\"_id\":\"Maxime\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}"),
        ("John",
          "{\"_id\":\"John\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}"),
        ("David",
          "{\"_id\":\"David\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}"),
        ("Andrew", "{\"_id\":\"Andrew\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
      ))) {
      println("testingSplitPartitioner succeded")
      true
    } else {
      println("testingSplitPartitioner failed")
      saved_profiles
        .map(a => (a.getIdString, a.asJsonString))
        .foreach(println(_))
      false
    }
  }

  def testingSplitPartitionerWithBinaryData(sc: SparkContext,
                                            tableName: String,
                                            saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable + "SplitpartitionerWithBinary"))
      MapRDB.deleteTable(saveToTable + "SplitpartitionerWithBinary")

    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .keyBy(a =>
        MapRDBSpark.serializableBinaryValue(ByteBuffer.wrap(
          a.`first_name`[String].getBytes(Charset.forName("UTF-8")))))
      .repartitionAndSortWithinPartitions(MapRDBSpark.newPartitioner(Seq(
        MapRDBSpark.serializableBinaryValue(
          ByteBuffer.wrap("Bolo".getBytes(Charset.forName("UTF-8")))),
        MapRDBSpark.serializableBinaryValue(
          ByteBuffer.wrap("zebra".getBytes(Charset.forName("UTF-8"))))
      )))
    cust_profiles.saveToMapRDB(saveToTable + "SplitpartitionerWithBinary",
      bulkInsert = true,
      createTable = true)
    val saved_profiles =
      sc.loadFromMapRDB(saveToTable + "SplitpartitionerWithBinary").collect
    if (saved_profiles
      .map(a => a.delete("_id"))
      .map(a => a.asJsonString)
      .toSet
      .equals(Set(
        "{\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
        "{\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
        "{\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
        "{\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}"
      ))) {
      println("testingSplitPartitionerWithBinaryData succeded")
      true
    } else {
      println("testingSplitPartitionerWithBinaryData failed")
      saved_profiles
        .map(a => (a.getIdBinary, a.asJsonString))
        .foreach(println(_))
      false
    }
  }

  def testingSplitPartitionerWithByteBufferData(sc: SparkContext,
                                                tableName: String,
                                                saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable + "SplitpartitionerWithByteBuffer"))
      MapRDB.deleteTable(saveToTable + "SplitpartitionerWithByteBuffer")

    val cust_profiles = sc
      .loadFromMapRDB(tableName)
      .keyBy(a =>
        ByteBuffer.wrap(
          a.`first_name`[String].getBytes(Charset.forName("UTF-8"))))
      .repartitionAndSortWithinPartitions(
        MapRDBSpark.newPartitioner[ByteBuffer](Seq(
          MapRDBSpark
            .serializableBinaryValue(
              ByteBuffer.wrap("Bolo".getBytes(Charset.forName("UTF-8"))))
            .getByteBuffer(),
          MapRDBSpark
            .serializableBinaryValue(
              ByteBuffer.wrap("zebra".getBytes(Charset.forName("UTF-8"))))
            .getByteBuffer()
        )))
    cust_profiles.saveToMapRDB(saveToTable + "SplitpartitionerWithByteBuffer",
      bulkInsert = true,
      createTable = true)
    val saved_profiles =
      sc.loadFromMapRDB(saveToTable + "SplitpartitionerWithByteBuffer").collect
    if (saved_profiles
      .map(a => a.delete("_id"))
      .map(a => a.asJsonString)
      .toSet
      .equals(Set(
        "{\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
        "{\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
        "{\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
        "{\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}"
      ))) {
      println("testingSplitPartitionerWithByteBufferData succeded")
      true
    } else {
      println("testingSplitPartitionerWithByteBufferData failed")
      saved_profiles
        .map(a => (a.getIdBinary, a.asJsonString))
        .foreach(println(_))
      false
    }
  }

  def testingSimpleSaveModeBeanClass(sc: SparkContext,
                                     tableName: String,
                                     saveToTable: String) = {
    if (MapRDB.tableExists(saveToTable + "simplebean"))
      MapRDB.deleteTable(saveToTable + "simplebean")
    val cust_profiles: RDD[BeanTest.User1] = sc.loadFromMapRDB[BeanTest.User1](
      tableName) //.map(user => BeanTest.User1(user.id,user.firstName,Option(1000),user.dob,user.interests))
    cust_profiles.saveToMapRDB(saveToTable + "simplebean", createTable = true)
    val saved_profiles = sc.loadFromMapRDB(saveToTable + "simplebean").collect
    if (saved_profiles
      .map(a => (a.getIdString, a.asJsonString()))
      .toSet
      .sameElements(Set(
        ("rsmith",
          "{\"_id\":\"rsmith\",\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}"),
        ("mdupont",
          "{\"_id\":\"mdupont\",\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}"),
        ("jdoe",
          "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"interests\":null,\"last_name\":\"Doe\"}"),
        ("dsimon",
          "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"interests\":null,\"last_name\":\"Simon\"}"),
        ("alehmann",
          "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
      ))) {
      println("testingSimpleSaveModeBeanClass succeded")
      true
    } else {
      println("testingSimpleSaveModeBeanClass failed")
      saved_profiles
        .map(a => (a.getIdString, a.asJsonString))
        .foreach(println(_))
      false
    }
  }
}

object LoadAndSaveTestsWithKryo {
  lazy val conf = new SparkConf()
    .setAppName("simpletest")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")
  val tableName = "/tmp/user_profiles_load_and_save_read_kryo"
  val saveToTable = "/tmp/user_profiles_load_and_save_write_kryo"

  def main(args: Array[String]): Unit = {
    var sc = new SparkContext(conf)
    LoadAndSaveTests.tableInitialization(sc, tableName)
    LoadAndSaveTests.runTests(sc, true)
  }
}
