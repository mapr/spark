/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.testCases

import org.apache.spark.{SparkConf, SparkContext}
import org.ojai.types.{ODate, OTime}
import com.mapr.db.spark._
import com.mapr.db.spark.field
import org.ojai.Document
import org.apache.spark.rdd.RDD
import org.ojai.exceptions.TypeException
import org.ojai.store.QueryCondition
import com.mapr.db.MapRDB

object PredicateTests {
  val tableName = "/tmp/user_profiles_predicates"

  lazy val conf = new SparkConf()
    .setAppName("simpletest")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")

  def main(args: Array[String]): Unit = {
    var sc = new SparkContext(conf)
    tableInitialization(sc, tableName)
    runTests(sc)
  }

  def tableInitialization(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists(tableName))
      MapRDB.deleteTable(tableName)
    println("table successfully create :" + tableName)
    MapRDB.createTable(tableName)
  }

  def runTests(sparkSession: SparkContext): Unit = {
    testingIdOnlyCondition(sparkSession, tableName)
    testingSimpleGTCondition(sparkSession, tableName)
    testingNotExistsCondition(sparkSession, tableName)
    testingSimpleINCondition(sparkSession, tableName)
    testingTYPEOFCondition(sparkSession, tableName)
    testingComplexAND_INcondition(sparkSession, tableName)
    testingCompositeCondition(sparkSession, tableName)
    testingMatchesCondition(sparkSession, tableName)
    testingLikeCondition(sparkSession, tableName)
    testingThreeConditions(sparkSession, tableName)
    testingORCondition(sparkSession, tableName)
    testingComplexConditonWithDate(sparkSession, tableName)
    testingBetweenCondition(sparkSession, tableName)
    testingEqualityConditionOnSeq(sparkSession, tableName)
    testingEqualtiyOnMapOfStrings(sparkSession, tableName)
    testingEqualityOnMapStringInteger(sparkSession, tableName)
    testingNotEqualityOnID(sparkSession, tableName)
    testingNotEqualityConditionOnSeq(sparkSession, tableName)
    testingNotEqualityOnMapStringInteger(sparkSession, tableName)
    testingSizeOf(sparkSession, tableName)
    testingSizeOfWithComplexCondition(sparkSession, tableName)
    testingTypeOfWithNonExistantType(sparkSession, tableName)
    testingWithQueryCondition(sparkSession, tableName)
    testWithListINCondition(sparkSession, tableName)
    testingSizeOfNotEquals(sparkSession, tableName)
    testingINConditionOnSeqwithInSeq(sparkSession, tableName)
  }

  def testingIdOnlyCondition(sparkSession : SparkContext , tableName : String) = {
    val d: Document = MapRDB.newDocument.set("a.b", 350).set("s", "Holger Way")
    MapRDB.getTable(tableName).insertOrReplace("k1", d)
    d.set("zip-code", "95134")
    MapRDB.getTable(tableName).insertOrReplace("k2", d)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("_id") === "k2").collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    if (maprd.map(doc => doc.`zip-code`[String]).sameElements(Array("95134"))) {
      println("testingIdOnlyCondition succeeded")
      true
    } else {
      println("testingIdOnlyCondition failed")
      false
    }
  }

  def testingSimpleGTCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b[0].boolean", false).set("a.b[1].decimal", 123.456).set("a.c.d", 5.0).set("a.c.e", "Hello").set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", 11.0)
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", 8.0)
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    doc.set("a.c.d", 14.0)
    MapRDB.getTable(tableName).insertOrReplace("k4", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("a.c.d") > 10).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    MapRDB.getTable(tableName).delete("k4")
    if (maprd.map(doc => doc.`a.c.d`[Double]).toSet.equals(Set(11.0d,14.0d))) {
      println("testingSimpleGTCondition succeeded")
      true
    } else {
      println("testingSimpleGTCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingNotExistsCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b[0].boolean", false).set("a.b[1].decimal", 123.456).set("a.c.d", 5.0).set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.e", "SanJose")
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.e", "Milpitas")
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    MapRDB.getTable(tableName).insertOrReplace("k4", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("a.c.e") notexists).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    MapRDB.getTable(tableName).delete("k4")
    if (maprd.map(doc => doc.getIdString()).sameElements((Array("k1")))) {
      println("testingNotExistsCondition succeeded")
      true
    } else {
      println("testingNotExistsCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingSimpleINCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.boolean", false).set("a.c.d", ODate.parse("2010-01-11")).set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", ODate.parse("2011-05-21"))
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", ODate.parse("2005-06-21"))
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("a.c.d") in Seq(ODate.parse("2011-05-21"), ODate.parse("2013-02-22"))).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("k2"))) {
      println("testingSimpleINCondition succeeded")
      true
    } else {
      println("testingSimpleINCondition failed")
      false
    }
  }

  def testingOTimeINCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.boolean", false).set("a.c.d", new OTime(1000)).set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", new OTime(1001))
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", new OTime(1002))
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("a.c.d") in Seq(new OTime(999), new OTime(1000))).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("k1"))) {
      println("testingOTimeINCondition succeeded")
      true
    } else {
      println("testingOTimeINCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingSimpleOTime(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.boolean", false).set("a.c.d", new OTime(1000)).set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", new OTime(1001))
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", new OTime(1002))
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("a.c.d") === new OTime(1000)).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("k1"))) {
      println("testingSimpleOTime succeeded")
      true
    } else {
      println("testingSimpleOTime failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingTYPEOFCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b[0].boolean", false).set("a.b[1].decimal", 123.456).set("a.c.d", 5).set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", "SanJose")
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", "Milpitas")
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("a.c.d") typeof "INT").collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("k1"))) {
      println("testingTYPEOFCondition succeeded")
      true
    } else {
      println("testingTYPEOFCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingComplexAND_INcondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b[0].boolean", false).set("a.b[1].decimal", 123.456).set("a.c.d", 5).set("a.c.e", "aaa").set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    doc.set("a.c.e", "xyz")
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where((field("a.c.d") in Seq(5,10)) and (field("a.c.e") notin Seq("aaa","bbb"))).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    if (maprd.map(doc =>doc.getIdString()).sameElements(Array("k2"))) {
      println("testingComplexAND_INcondition succeeded")
      true
    } else {
      println("testingComplexAND_INcondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingCompositeCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b[0].boolean", false).set("a.b[1].decimal", 123.456).set("a.c.d", 5.0).set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", 10.0)
    doc.set("a.c.e", "xyz")
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", 8.0)
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where((field("a.c.d") < 10) and (field("a.c.e") exists)).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    if (maprd.map(doc => doc.getIdString()).sameElements((Array("k3")))) {
      println("testingCompositeCondition succeeded")
      true
    } else {
      println("testingCompositeCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingLikeCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b[0].boolean", false).set("a.b[1].decimal", 123.456).set("a.c.d", 5.0).set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", 10.0)
    doc.set("a.c.e", "xyz")
    doc.set("m", "xyz")
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", 8.0)
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("m") like "%s").collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    if (maprd.map(doc => doc.getIdString()).sameElements((Array("k1")))) {
      println("testingLikeCondition succeeded")
      true
    } else {
      println("testingLikeCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }


  def testingMatchesCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b[0].boolean", false).set("a.b[1].decimal", 123.456).set("a.c.d", 5.0).set("m", "MapR wins")
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", 10.0)
    doc.set("a.c.e", "xyz")
    doc.set("m", "xyz")
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", 8.0)
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("m") matches ".*s").collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    if (maprd.map(doc => doc.getIdString()).sameElements((Array("k1")))) {
      println("testingMatchesCondition succeeded")
      true
    } else {
      println("testingMatchesCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingThreeConditions(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b", 5.1).set("a.c.e", "mapR").set("m", "MapR wins").set("a.k", 25.1)
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.k", 10.11)
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where((field("a.c.e") exists) and ((field("a.b") >= 5) and (field("a.k") < 20))).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("k2"))) {
      println("testingThreeConditions succeeded")
      true
    } else {
      println("testingThreeConditions failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))

      false
    }
  }

  def testingORCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b", 5.1).set("a.c.d", "mapR").set("m", "MapR wins").set("a.k", 25.1)
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", 101)
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", 100)
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    doc.set("a.c.d", 99)
    MapRDB.getTable(tableName).insertOrReplace("k4", doc)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where((field("a.c.d") > 100L) or (field("a.c.d") typeof "STRING")).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    MapRDB.getTable(tableName).delete("k4")
    if (maprd.map(doc => 1).reduce(_+_) == 2) {
      println("testingORCondition succeeded")
      true
    } else {
      println("testingORCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingTypeOfWithNonExistantType(sparkSession: SparkContext, tableName: String) = {
    var succeeded : Boolean = false;
    val doc: Document = MapRDB.newDocument.set("a.b", 5.1).set("a.c.d", "mapR").set("m", "MapR wins").set("a.k", 25.1)
    MapRDB.getTable(tableName).insertOrReplace("k1", doc)
    doc.set("a.c.d", 101)
    MapRDB.getTable(tableName).insertOrReplace("k2", doc)
    doc.set("a.c.d", 100)
    MapRDB.getTable(tableName).insertOrReplace("k3", doc)
    doc.set("a.c.d", 99)
    MapRDB.getTable(tableName).insertOrReplace("k4", doc)
    MapRDB.getTable(tableName).flush
    try {
      val maprd = sparkSession.loadFromMapRDB(tableName).where((field("a.c.d") > 100L) or (field("a.c.d") typeof "STR")).collect
    } catch {
      case e: TypeException => println(e.getMessage)
        println("testingTypeOfWithNonExistantType succeeded")
        succeeded = true
    }
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    MapRDB.getTable(tableName).delete("k3")
    MapRDB.getTable(tableName).delete("k4")
    if (succeeded) {
      println("testingTypeOfWithNonExistantType succeeded")
      true
    } else {
      println("testingTypeOfWithNonExistantType failed")
      false
    }
  }

  def testingComplexConditonWithDate(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.c.d", 22).set("m", "MapR wins").set("p.q", ODate.parse("2016-04-05"))
    MapRDB.getTable(tableName).insertOrReplace("id1", doc)
    doc.set("a.b", "xyz")
    MapRDB.getTable(tableName).insertOrReplace("id2", doc)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(((field("a.b") notexists ) and (field("p.q") typeof "DATE")) and (field("a.c.d") > 20L)).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => doc.getIdString()).sameElements((Array("id1")))) {
      println("testingComplexConditionWithDate succeeded")
      true
    } else {
      println("testingComplexConditionWithDate failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingBetweenCondition(sparkSession: SparkContext, tableName: String) = {
    val doc: Document = MapRDB.newDocument.set("a.b[0].boolean", false).set("a.c.d", ODate.parse("2015-01-16")).set("a.c.e", "Hello").set("m", "MapR")
    MapRDB.getTable(tableName).insertOrReplace("id1", doc)
    doc.set("a.c.d", ODate.parse("2015-01-14"))
    MapRDB.getTable(tableName).insertOrReplace("id2", doc)
    doc.set("a.c.d", ODate.parse("2015-06-14"))
    MapRDB.getTable(tableName).insertOrReplace("id3", doc)
    doc.set("a.c.d", ODate.parse("2015-02-26"))
    MapRDB.getTable(tableName).insertOrReplace("id4", doc)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(field("a.c.d") between (ODate.parse("2015-01-15"), ODate.parse("2015-05-15"))).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    MapRDB.getTable(tableName).delete("id3")
    MapRDB.getTable(tableName).delete("id4")
    if (maprd.map(doc => doc.getIdString()).toSet.equals(Set("id1", "id4"))) {
      println("testingBetweenCondition succeeded")
      true
    } else {
      println("testingBetweenCondition failed")
      maprd.map(doc => doc.getIdString()).foreach(println(_))
      false
    }
  }

  def testingEqualityConditionOnSeq(sparkSession: SparkContext, tableName: String) = {
    val d: Document = MapRDB.newDocument.set("a.b", 111.222).set("a.x", true)
    MapRDB.getTable(tableName).insertOrReplace("id1", d)
    val d2: Document = d.set("a.b[0]", 12345).set("a.b[1]", "xyz")
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(field("a.b") === Seq(12345L, "xyz")).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => (doc.getIdString(),doc.`a.b`[Seq[AnyRef]].apply(0),doc.`a.b`[Seq[AnyRef]].apply(1))).toSet.equals(Set(("id2", 12345L, "xyz")))) {
      println("testingEqualityConditionOnSeq succeeded")
      true
    } else {
      println("testingEqualityConditionSeq failed")
      maprd.map(doc => (doc.getIdString(),doc.getList("a.b")(0),doc.getList("a.b")(1))).foreach(println(_))
      false
    }
  }

  def testingINConditionOnSeqwithInSeq(sparkSession: SparkContext, tableName: String) = {
    val d: Document = MapRDB.newDocument.set("a.b", 111.222).set("a.x", true)
    MapRDB.getTable(tableName).insertOrReplace("id1", d)
    val d2: Document = d.set("a.b1[0]", 12345).set("a.b1[1]", "xyz")
    d2.set("a.c[0]", d.getList("a.b1"))
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val list: Seq[Any] = sparkSession.loadFromMapRDB(tableName).map(doc => doc.`a.b1`).collect
    val maprd = sparkSession.loadFromMapRDB(tableName).where(field("a.c") === Seq(list(1))).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    val values = maprd.map(doc => doc.getIdString())
    if (values.toSet.equals(Set("id2"))) {
      println("testingINConditionOnSeqwithInSeq succeeded")
      true
    } else {
      println("testingINConditionOnSeqwithInSeq failed")
      maprd.map(doc => (doc.getIdString(),doc.getList("a.b")(0),doc.getList("a.b")(1))).foreach(println(_))
      false
    }
  }

  def testingNotEqualityConditionOnSeq(sparkSession: SparkContext, tableName: String) = {
    val d: Document = MapRDB.newDocument.set("a.b", 111.222).set("a.x", true)
    MapRDB.getTable(tableName).insertOrReplace("id1", d)
    val d2: Document = d.set("a.b[0]", 12345).set("a.b[1]", "xyz")
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(field("a.b") != Seq(12345L, "xyz")).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => (doc.getIdString(),doc.`a.b`[Double])).toSet.equals(Set(("id1",111.222)))) {
      println("testingNotEqualityConditionOnSeq succeeded")
      true
    } else {
      println("testingNotEqualityConditionSeq failed")
      maprd.map(doc => (doc.getIdString(),doc.getList("a.b")(0),doc.getList("a.b")(1))).foreach(println(_))
      false
    }
  }

  def testingNotEqualityOnID(sparkSession : SparkContext , tableName : String) = {
    val d: Document = MapRDB.newDocument.set("a.b", 350).set("s", "Holger Way")
    MapRDB.getTable(tableName).insertOrReplace("k1", d)
    d.set("zip-code", "95134")
    MapRDB.getTable(tableName).insertOrReplace("k2", d)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(field("_id") != "k1").collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    if (maprd.map(doc => doc.`zip-code`[String]).sameElements(Array("95134"))) {
      println("testingIdOnlyCondition succeeded")
      true
    } else {
      println("testingIdOnlyCondition failed")
      false
    }
  }

  def testingEqualtiyOnMapOfStrings(sparkSession: SparkContext, tableName: String) = {
    val m: java.util.Map[String, String] = new java.util.HashMap[String, String]
    m.put("k", "kite")
    m.put("m", "map")
    val doc: Document = MapRDB.newDocument.set("a", m).set("b.c.d", ODate.parse("2013-03-22"))
    MapRDB.getTable(tableName).insertOrReplace("id1", doc)
    val d2: Document = doc.set("a.m", "not map").setArray("x.y", Array[Int](4, 44))
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(field("a") === Map("k" -> "kite", "m" -> "map")).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("id1"))) {
      println("testingEqualityOnMapOfString succeeded")
      true
    } else {
      println("testingEqualityOnMapOfStrings failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingEqualityOnMapStringInteger(sparkSession: SparkContext, tableName: String) = {
    val m: java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]
    m.put("k", 100)
    m.put("m", 120)
    val doc: Document = MapRDB.newDocument.set("a", m).set("b.c.d", ODate.parse("2013-03-22"))
    MapRDB.getTable(tableName).insertOrReplace("id1", doc)
    val d2: Document = doc.set("a.m", "not map").setArray("x.y", Array[Int](4, 44))
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(field("a") === Map("k" -> 100, "m" -> 120)).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("id1"))) {
      println("testingEqualityOnMapStringInteger succeeded")
      true
    } else {
      println("testingEqualityOnMapStringInteger failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingNotEqualityOnMapStringInteger(sparkSession: SparkContext, tableName: String) = {
    val m: java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]
    m.put("k", 100)
    m.put("m", 120)
    val doc: Document = MapRDB.newDocument.set("a", m).set("b.c.d", ODate.parse("2013-03-22"))
    MapRDB.getTable(tableName).insertOrReplace("id1", doc)
    val d2: Document = doc.set("a.m", "not map").setArray("x.y", Array[Int](4, 44))
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(field("a") != Map("k" -> 100, "m" -> 120)).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("id2"))) {
      println("testingNotEqualityOnMapStringInteger succeeded")
      true
    } else {
      println("testingNotEqualityOnMapStringInteger failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }


  def testingSizeOf(sparkSession: SparkContext, tableName: String) = {
    val m: java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]
    m.put("k", 100)
    m.put("m", 120)
    val doc: Document = MapRDB.newDocument.set("a", m).set("b.c.d", ODate.parse("2013-03-22"))
    MapRDB.getTable(tableName).insertOrReplace("id1", doc)
    val d2: Document = doc.set("a.m", "not map").setArray("x.y", Array[Int](4, 44))
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(sizeOf(field("a.m")) > 4).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("id2"))) {
      println("testingSizeOf succeeded")
      true
    } else {
      println("testingSizeOf failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingSizeOfWithComplexCondition(sparkSession: SparkContext, tableName: String) = {
    val m: java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]
    m.put("k", 100)
    m.put("m", 120)
    val doc: Document = MapRDB.newDocument.set("a", m).set("b.c.d", ODate.parse("2013-03-22"))
    MapRDB.getTable(tableName).insertOrReplace("id1", doc)
    val d2: Document = doc.set("a.m", "not map").setArray("x.y", Array[Int](4, 44))
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where((sizeOf(field("a")) > 1) and (field("a") != Map("k" -> 100, "m" -> 120))).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("id2"))) {
      println("testingSizeOfWithComplexCondition succeeded")
      true
    } else {
      println("testingSizeOfWithComplexCondition failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }

  def testingWithQueryCondition(sparkSession: SparkContext, tableName: String) = {
    val d: Document = MapRDB.newDocument.set("a.b", 350).set("s", "Holger Way")
    MapRDB.getTable(tableName).insertOrReplace("k1", d)
    d.set("zip-code", "95134")
    MapRDB.getTable(tableName).insertOrReplace("k2", d)
    MapRDB.getTable(tableName).flush
    val maprd  = sparkSession.loadFromMapRDB(tableName).where(MapRDB.newCondition().is("_id", QueryCondition.Op.EQUAL, "k2").build()).collect
    MapRDB.getTable(tableName).delete("k1")
    MapRDB.getTable(tableName).delete("k2")
    if (maprd.map(doc => doc.`zip-code`[String]).sameElements(Array("95134"))) {
      println("testingWithQueryCondition succeeded")
      true
    } else {
      println("testingWithQueryCondition failed")
      false
    }
  }

  def testWithListINCondition(sparkSession: SparkContext, tableName: String) = {
    val maprd  = sparkSession.loadFromMapRDB("/tmp/bug26114").where(field("level0_a0_bool") in List(-4998511681057838802L, 0.22987476349110803, -1722097687, 0.6030484, false)).collect
    if (maprd.size == 0) {
      println("testWithListINCondition succeeded")
      true
    } else {
      println("testWithListINCondition failed")
      false
    }
  }

  def testingSizeOfNotEquals(sparkSession: SparkContext, tableName: String) = {
    val m: java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]
    m.put("k", 100)
    m.put("m", 120)
    val doc: Document = MapRDB.newDocument.set("a", m).set("b.c.d", ODate.parse("2013-03-22"))
    MapRDB.getTable(tableName).insertOrReplace("id1", doc)
    val d2: Document = doc.set("a.m", "not map").setArray("x.y", Array[Int](4, 44))
    MapRDB.getTable(tableName).insertOrReplace("id2", d2)
    MapRDB.getTable(tableName).flush
    val maprd = sparkSession.loadFromMapRDB(tableName).where(sizeOf(field("a.m")) != 4).collect
    MapRDB.getTable(tableName).delete("id1")
    MapRDB.getTable(tableName).delete("id2")
    if (maprd.map(doc => doc.getIdString()).sameElements(Array("id2"))) {
      println("testingSizeOfNotEquals succeeded")
      true
    } else {
      println("testingSizeOfNotEquals failed")
      maprd.map(doc => doc.asJsonString()).foreach(println(_))
      false
    }
  }
}


object PredicateTestsWithKryo {
  val tableName = "/tmp/user_profiles_predicates"
  lazy val conf = new SparkConf()
    .setAppName("simpletest")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")

  def main(args: Array[String]): Unit = {
    var sc = new SparkContext(conf)
    PredicateTests.tableInitialization(sc, tableName)
    PredicateTests.runTests(sc)
  }
}
