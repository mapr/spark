/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.testCases

import org.apache.spark.{SparkConf, SparkException}
import org.ojai.Document
import java.math.BigDecimal
import java.sql.Timestamp
import com.mapr.db.rowcol.DBDocumentImpl
import com.mapr.db.spark._
import com.mapr.db.spark.exceptions.SchemaMappingException
import org.apache.spark.sql.SparkSession
import org.ojai.types.{ODate, OTime, OTimestamp}
import com.mapr.org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator
import com.mapr.db.MapRDB
import com.mapr.db.spark.dbclient.DBClient

object SparkSqlAccessTests {
  lazy val conf = new SparkConf()
    .setAppName("simpletest")
    .set("spark.executor.memory","1g")
    .set("spark.driver.memory", "1g")

  lazy val spark = SparkSession.builder().appName("SparkSqlTest").config(conf).getOrCreate()

  val tableName="/tmp/SparkSqlOjaiConnectorAccessTesting"

  def main(args: Array[String]): Unit = {
    SparkSqlAccessTests.tableInitialization(tableName)
    SparkSqlAccessTests.runTests(spark)
  }

  def tableInitialization(tableName: String): Unit = {
    if (MapRDB.tableExists(tableName))
      MapRDB.deleteTable(tableName)
    val tabDesc = DBClient().newTableDescriptor()
    tabDesc.setAutoSplit(true)
    tabDesc.setPath(tableName)
    tabDesc.setInsertionOrder(false)
    DBClient().createTable(tabDesc)
    val table = DBClient().getTable(tableName)
    table.insertOrReplace(getNullRecord())
    table.insertOrReplace(getBooleanRecord())
    table.insertOrReplace(getStringRecord())
    table.insertOrReplace(getByteRecord())
    table.insertOrReplace(getShortRecord())
    table.insertOrReplace(getIntRecord())
    table.insertOrReplace(getLongRecord())
    table.insertOrReplace(getFloatRecord())
    table.insertOrReplace(getDoubleRecord())
    //table.insertOrReplace(getDecimalRecord())
    table.insertOrReplace(getDateRecord())
    table.insertOrReplace(getTimeRecord())
    table.insertOrReplace(getTimeStampRecord())
    table.insertOrReplace(getBinaryRecord())
    table.insertOrReplace(getMapRecord())
    table.insertOrReplace(getArrayRecord())
    table.flush()
    table.close()
  }

  private def getBooleanRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("boolean")
    rec.set("null", true)
    rec.set("boolean", true)
    rec.set("string", true)
    rec.set("byte", true)
    rec.set("short", true)
    rec.set("int", true)
    rec.set("long", true)
    rec.set("float", true)
    rec.set("double", true)
    rec.set("decimal", true)
    rec.set("date", true)
    rec.set("time", true)
    rec.set("timestamp", true)
    rec.set("binary", true)
    rec.set("map", true)
    rec.set("array", true)
    return rec
  }

  private def getNullRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("null")
    rec.setNull("null")
    rec.setNull("boolean")
    rec.setNull("string")
    rec.setNull("byte")
    rec.setNull("short")
    rec.setNull("int")
    rec.setNull("long")
    rec.setNull("float")
    rec.setNull("double")
    rec.setNull("decimal")
    rec.setNull("date")
    rec.setNull("time")
    rec.setNull("timestamp")
    rec.setNull("binary")
    rec.setNull("map")
    rec.setNull("array")
    return rec
  }

  private def getStringRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("string")
    rec.set("null", "hello")
    rec.set("boolean", "hello")
    rec.set("string", "hello")
    rec.set("byte", "hello")
    rec.set("short", "hello")
    rec.set("int", "hello")
    rec.set("long", "hello")
    rec.set("float", "hello")
    rec.set("double", "hello")
    rec.set("decimal", "hello")
    rec.set("date", "hello")
    rec.set("time", "hello")
    rec.set("timestamp", "hello")
    rec.set("binary", "hello")
    rec.set("map", "hello")
    rec.set("array", "hello")
    return rec
  }

  private def getByteRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("byte")
    rec.set("null", 100.toByte)
    rec.set("boolean", 100.toByte)
    rec.set("string", 100.toByte)
    rec.set("byte", 100.toByte)
    rec.set("short", 100.toByte)
    rec.set("int", 100.toByte)
    rec.set("long", 100.toByte)
    rec.set("float", 100.toByte)
    rec.set("double", 100.toByte)
    rec.set("decimal", 100.toByte)
    rec.set("date", 100.toByte)
    rec.set("time", 100.toByte)
    rec.set("timestamp", 100.toByte)
    rec.set("binary", 100.toByte)
    rec.set("map", 100.toByte)
    rec.set("array", 100.toByte)
    return rec
  }


  private def getShortRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("short")
    rec.set("null", 10000.toShort)
    rec.set("boolean", 10000.toShort)
    rec.set("string", 10000.toShort)
    rec.set("byte", 10000.toShort)
    rec.set("short", 10000.toShort)
    rec.set("int", 10000.toShort)
    rec.set("long", 10000.toShort)
    rec.set("float", 10000.toShort)
    rec.set("double", 10000.toShort)
    rec.set("decimal", 10000.toShort)
    rec.set("date", 10000.toShort)
    rec.set("time", 10000.toShort)
    rec.set("timestamp", 10000.toShort)
    rec.set("binary", 10000.toShort)
    rec.set("map", 10000.toShort)
    rec.set("array", 10000.toShort)
    return rec
  }

  private def getIntRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("int")
    rec.set("null", new Integer(5000))
    rec.set("boolean", new Integer(5000))
    rec.set("string", new Integer(5000))
    rec.set("byte", new Integer(5000))
    rec.set("short", new Integer(5000))
    rec.set("int", new Integer(5000))
    rec.set("long", new Integer(5000))
    rec.set("float", new Integer(5000))
    rec.set("double", new Integer(5000))
    rec.set("decimal", new Integer(5000))
    rec.set("date", new Integer(5000))
    rec.set("time", new Integer(5000))
    rec.set("timestamp", new Integer(5000))
    rec.set("binary", new Integer(5000))
    rec.set("map", new Integer(5000))
    rec.set("array", new Integer(5000))
    return rec
  }

  private def getLongRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("long")
    rec.set("null", 12345678999L)
    rec.set("boolean", 12345678999L)
    rec.set("string", 12345678999L)
    rec.set("byte", 12345678999L)
    rec.set("short", 12345678999L)
    rec.set("int", 12345678999L)
    rec.set("long", 12345678999L)
    rec.set("float", 12345678999L)
    rec.set("double", 12345678999L)
    rec.set("decimal", 12345678999L)
    rec.set("date", 12345678999L)
    rec.set("time", 12345678999L)
    rec.set("timestamp", 12345678999L)
    rec.set("binary", 12345678999L)
    rec.set("map", 12345678999L)
    rec.set("array", 12345678999L)
    return rec
  }

  private def getFloatRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("float")
    rec.set("null", 10.1234f)
    rec.set("boolean", 10.1234f)
    rec.set("string", 10.1234f)
    rec.set("byte", 10.1234f)
    rec.set("short", 10.1234f)
    rec.set("int", 10.1234f)
    rec.set("long", 10.1234f)
    rec.set("float", 10.1234f)
    rec.set("double", 10.1234f)
    rec.set("decimal", 10.1234f)
    rec.set("date", 10.1234f)
    rec.set("time", 10.1234f)
    rec.set("timestamp", 10.1234f)
    rec.set("binary", 10.1234f)
    rec.set("map", 10.1234f)
    rec.set("array", 10.1234f)
    return rec
  }

  private def getDoubleRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("double")
    rec.set("null", 10.12345678910d)
    rec.set("boolean", 10.12345678910d)
    rec.set("string", 10.12345678910d)
    rec.set("byte", 10.12345678910d)
    rec.set("short", 10.12345678910d)
    rec.set("int", 10.12345678910d)
    rec.set("long", 10.12345678910d)
    rec.set("float", 10.12345678910d)
    rec.set("double", 10.12345678910d)
    rec.set("decimal", 10.12345678910d)
    rec.set("date", 10.12345678910d)
    rec.set("time", 10.12345678910d)
    rec.set("timestamp", 10.12345678910d)
    rec.set("binary", 10.12345678910d)
    rec.set("map", 10.12345678910d)
    rec.set("array", 10.12345678910d)
    return rec
  }


  private def getDecimalRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("decimal")
    rec.set("null", new BigDecimal(1.5))
    rec.set("boolean", new BigDecimal(1.5))
    rec.set("string", new BigDecimal(1.5))
    rec.set("byte", new BigDecimal(1.5))
    rec.set("short", new BigDecimal(1.5))
    rec.set("int", new BigDecimal(1.5))
    rec.set("long", new BigDecimal(1.5))
    rec.set("float", new BigDecimal(1.5))
    rec.set("double", new BigDecimal(1.5))
    rec.set("decimal", new BigDecimal(1.5))
    rec.set("date", new BigDecimal(1.5))
    rec.set("time", new BigDecimal(1.5))
    rec.set("timestamp", new BigDecimal(1.5))
    rec.set("binary", new BigDecimal(1.5))
    rec.set("map", new BigDecimal(1.5))
    rec.set("array", new BigDecimal(1.5))
    return rec
  }


  private def getDateRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("date")
    rec.set("null", ODate.fromDaysSinceEpoch(1000))
    rec.set("boolean", ODate.fromDaysSinceEpoch(1000))
    rec.set("string", ODate.fromDaysSinceEpoch(1000))
    rec.set("byte", ODate.fromDaysSinceEpoch(1000))
    rec.set("short", ODate.fromDaysSinceEpoch(1000))
    rec.set("int", ODate.fromDaysSinceEpoch(1000))
    rec.set("long", ODate.fromDaysSinceEpoch(1000))
    rec.set("float", ODate.fromDaysSinceEpoch(1000))
    rec.set("double", ODate.fromDaysSinceEpoch(1000))
    rec.set("decimal", ODate.fromDaysSinceEpoch(1000))
    rec.set("date", ODate.fromDaysSinceEpoch(1000))
    rec.set("time", ODate.fromDaysSinceEpoch(1000))
    rec.set("timestamp", ODate.fromDaysSinceEpoch(1000))
    rec.set("binary", ODate.fromDaysSinceEpoch(1000))
    rec.set("map", ODate.fromDaysSinceEpoch(1000))
    rec.set("array", ODate.fromDaysSinceEpoch(1000))
    return rec
  }

  private def getTimeRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("time")
    rec.set("null", OTime.fromMillisOfDay(1000))
    rec.set("boolean", OTime.fromMillisOfDay(1000))
    rec.set("string", OTime.fromMillisOfDay(1000))
    rec.set("byte", OTime.fromMillisOfDay(1000))
    rec.set("short", OTime.fromMillisOfDay(1000))
    rec.set("int", OTime.fromMillisOfDay(1000))
    rec.set("long", OTime.fromMillisOfDay(1000))
    rec.set("float", OTime.fromMillisOfDay(1000))
    rec.set("double", OTime.fromMillisOfDay(1000))
    rec.set("decimal", OTime.fromMillisOfDay(1000))
    rec.set("date", OTime.fromMillisOfDay(1000))
    rec.set("time", OTime.fromMillisOfDay(1000))
    rec.set("timestamp", OTime.fromMillisOfDay(1000))
    rec.set("binary", OTime.fromMillisOfDay(1000))
    rec.set("map", OTime.fromMillisOfDay(1000))
    rec.set("array", OTime.fromMillisOfDay(1000))
    return rec
  }

  private def getTimeStampRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId("timestamp")
    rec.set("null", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("boolean", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("string", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("byte", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("short", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("int", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("long", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("float", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("double", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("decimal", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("date", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("time", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("timestamp", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("binary", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("map", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    rec.set("array", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr))
    return rec
  }

  private def getBinaryRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    val bytes: Array[Byte] = Array.range(1,10).map(_.toByte)
    rec.setId("binary")
    rec.set("null", bytes)
    rec.set("boolean", bytes)
    rec.set("string", bytes)
    rec.set("byte", bytes)
    rec.set("short", bytes)
    rec.set("int", bytes)
    rec.set("long", bytes)
    rec.set("float", bytes)
    rec.set("double", bytes)
    rec.set("decimal", bytes)
    rec.set("date", bytes)
    rec.set("time", bytes)
    rec.set("timestamp", bytes)
    rec.set("binary", bytes)
    rec.set("map", bytes)
    rec.set("array", bytes)
    return rec
  }

  private def getMapRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    val map:  java.util.Map [java.lang.String, Object] = new java.util.HashMap [java.lang.String, Object]()
    map.put("Name", "AAA")
    map.put("Age", new Integer(20))
    rec.setId("map")
    rec.set("null", map)
    rec.set("boolean", map)
    rec.set("string", map)
    rec.set("byte", map)
    rec.set("short", map)
    rec.set("int", map)
    rec.set("long", map)
    rec.set("float", map)
    rec.set("double", map)
    rec.set("decimal", map)
    rec.set("date", map)
    rec.set("time", map)
    rec.set("timestamp", map)
    rec.set("binary", map)
    rec.set("map", map)
    rec.set("array", map)
    return rec
  }

  private def getArrayRecord(): Document = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    val values = new java.util.ArrayList[Object]()
    values.add("Field1")
    val intvalue: Integer = 500
    values.add(intvalue)
    values.add(new java.lang.Double(5555.5555))
    rec.setId("array")
    rec.set("null", values)
    rec.set("boolean", values)
    rec.set("string", values)
    rec.set("byte", values)
    rec.set("short", values)
    rec.set("int", values)
    rec.set("long", values)
    rec.set("float", values)
    rec.set("double", values)
    rec.set("decimal", values)
    rec.set("date", values)
    rec.set("time", values)
    rec.set("timestamp", values)
    rec.set("binary", values)
    rec.set("map", values)
    rec.set("array", values)
    return rec
  }

  def testingBooleanVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where((field("_id") === "boolean") or (field("_id") === "null")).toDF
    if (booleanAndNullDF.collect.map(row => row.isNullAt(row.fieldIndex("boolean")) match {
      case true => null
      case false => row.getBoolean(row.fieldIndex("boolean"))
    }).toSet.equals(Set(true, null))) {
      println("testingBooleanVsNull Succeeded")
      return true
    }
    else {
      println("testingBooleanVsNull Failed")
      return false
    }
  }

  def testingStringVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", null))) {
      println("testingStringVsNull Succeeded")
      return true
    }
    else {
      println("testingStringVsNull Failed")
      return false
    }
  }

  def testingByteVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.isNullAt(row.fieldIndex("byte")) match { case true => null case false => row.getByte(row.fieldIndex("byte")) }).toSet.equals(Set(100.toByte, null))) {
      println("testingByteVsNull Succeeded")
      return true
    }
    else {
      println("testingByteVsNull Failed")
      return false
    }
  }

  def testingShortVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.isNullAt(row.fieldIndex("short")) match { case true => null case false => row.getShort(row.fieldIndex("short")) }).toSet.equals(Set(10000.toShort, null))) {
      println("testingShortVsNull Succeeded")
      return true
    }
    else {
      println("testingShortVsNull Failed")
      return false
    }
  }

  def testingIntVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.isNullAt(row.fieldIndex("int")) match { case true => null case false => row.getInt(row.fieldIndex("int")) }).toSet.equals(Set(5000.toInt, null))) {
      println("testingIntVsNull Succeeded")
      return true
    }
    else {
      println("testingIntVsNull Failed")
      return false
    }
  }

  def testingLongVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.isNullAt(row.fieldIndex("long")) match { case true => null case false => row.getLong(row.fieldIndex("long")) }).toSet.equals(Set(12345678999L, null))) {
      println("testingLongVsNull succeeded")
      return true
    }
    else {
      println("testingLongVsNull Failed")
      return false
    }
  }

  def testingFloatVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "float" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.isNullAt(row.fieldIndex("float")) match { case true => null case false => row.getFloat(row.fieldIndex("float"))}).toSet.equals(Set(10.1234f, null))) {
      println("testingFloatVsNull Succeeded")
      return true
    }
    else {
      println("testingFloatVsNull Failed")
      return false
    }
  }

  def testingDoubleVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "double" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.isNullAt(row.fieldIndex("double")) match { case true => null case false => row.getDouble(row.fieldIndex("double"))}).toSet.equals(Set(10.12345678910d, null))) {
      println("testingDoubleVsNull Succeeded")
      return true
    }
    else {
      println("testingDoubleVsNull Failed")
      return false
    }
  }

  def testingDateVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "date" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.getDate(row.fieldIndex("date"))).toSet.equals(Set(new java.sql.Date(ODate.fromDaysSinceEpoch(1000).toDate.getTime), null))) {
      println("testingDateVsNull Succeeded")
      return true
    }
    else {
      println("testingDateVsNull Failed")
      return false
    }
  }

  def testingTimeVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "time" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.getTimestamp(row.fieldIndex("time"))).toSet.equals(Set(new java.sql.Timestamp(OTime.fromMillisOfDay(1000).toDate.getTime), null))) {
      println("testingTimeVsNull Succeeded")
      return true
    }
    else {
      println("testingTimeVsNull Failed")
      return false
    }
  }

  def testingTimeStampVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "timestamp" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.getTimestamp(row.fieldIndex("timestamp"))).toSet.equals(Set(new Timestamp(OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr).getMillis), null))) {
      println("testingTimeStampVsNull Succeeded")
      return true
    }
    else {
      println("testingTimeSTampVsNull Failed")
      return false
    }
  }

  def testingBinaryVsNull(spark: SparkSession, tableName: String): Boolean = {
    val bytes: Array[Byte] = Array.range(1,10).map(_.toByte)
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "binary" or field("_id") === "null").toDF
    if (new ByteArrayComparator().compare(booleanAndNullDF.collect.map(row => row.get(row.fieldIndex("binary")).asInstanceOf[Array[Byte]]).filter(_ != null).toSeq(0), bytes) == 0) {
      println("testingBinaryVsNull Succeeded")
      return true
    }
    else {
      println("testingBinaryVsNull Failed")
      return false
    }
  }

  def testingMapVsNull(spark: SparkSession, tableName: String): Boolean = {
    val map = Map("Name" -> "AAA", "Age" -> 20.toInt)
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "map" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.getStruct(row.fieldIndex("map"))).filter(_!=null)
      .map(row => row.schema.fieldNames.zip(row.toSeq).toMap)
      .toSeq(0).toSeq.toSet.equals(map.toSeq.toSet)) {
      println("testingMapVsNull Succeeded")
      return true
    }
    else {
      println(booleanAndNullDF.collect.map(row => row.getMap(row.fieldIndex("map"))).filter(_!=null).toSeq(0).asInstanceOf[Map[String,Any]])
      println("testingMapVsNull Failed")
      return false
    }
  }

  def testingArrayVsNull(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "array" or field("_id") === "null").toDF
    if (booleanAndNullDF.collect.map(row => row.getSeq(row.fieldIndex("array"))).filter(_!=null).toSeq(0).asInstanceOf[Seq[Any]].sameElements(Seq("Field1","500","5555.5555"))) {
      println("testingArrayVsNull Succeeded")
      return true
    }
    else {
      println(booleanAndNullDF.collect.map(row => row.getSeq(row.fieldIndex("array"))).filter(_!=null).toSeq(0))
      println("testingArrayVsNull Failed")
      return false
    }
  }

  def testingBooleanVsString(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "string").toDF
    if (booleanAndNullDF.collect.map(row => row.isNullAt(row.fieldIndex("boolean")) match {
      case true => null
      case false => row.getString(row.fieldIndex("boolean"))
    }).toSet.equals(Set("true", "hello"))) {
      println("testingBooleanVsString Succeeded")
      return true
    }
    else {
      println("testingBooleanVsString Failed")
      return false
    }
  }

  def testingBooleanVsByte(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "byte").toDF.collect
      println("testingBooleanVsByte Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsByte Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsByte Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsByte Failed")
        return false
      }
    }
  }

  def testingBooleanVsShort(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "short").toDF.collect
      println("testingBooleanVsShort Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsShort Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsShort Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsShort Failed")
        return false
      }
    }
  }

  def testingBooleanVsInt(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "int").toDF.collect()
      println("testingBooleanVsInt Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsInt Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsInt Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsInt Failed")
        return false
      }
    }
  }

  def testingBooleanVsLong(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "long").toDF.collect
      println("testingBooleanVsLong Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsLong Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsLong Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsLong Failed")
        return false
      }
    }
  }

  def testingBooleanVsFloat(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "float").toDF.collect
      println("testingBooleanVsFloat Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsFloat Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsFloat Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsFloat Failed")
        return false
      }
    }
  }

  def testingBooleanVsDouble(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "double").toDF.collect
      println("testingBooleanVsDouble Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsDouble Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsDouble Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsDouble Failed")
        return false
      }
    }
  }

  def testingBooleanVsDate(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "date").toDF.collect
      println("testingBooleanVsDate Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsDate Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsDate Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsDate Failed")
        return false
      }
    }
  }

  def testingBooleanVsTime(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "time").toDF.collect
      println("testingBooleanVsTime Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsTime Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsTime Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsTime Failed")
        return false
      }
    }
  }

  def testingBooleanVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "timestamp").toDF.collect
      println("testingBooleanVsTimeStamp Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsTimeStamp Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsTimeStamp Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsTimeStamp Failed")
        return false
      }
    }
  }

  def testingBooleanVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "binary").toDF.collect
      println("testingBooleanVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsBinary Failed")
        return false
      }
    }
  }

  def testingBooleanVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "map").toDF.collect
      println("testingBooleanVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsMap Failed")
        return false
      }
    }
  }

  def testingBooleanVsArray(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "boolean" or field("_id") === "array").toDF.collect
      println("testingBooleanVsArray Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBooleanVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBooleanVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBooleanVsArray Failed")
        return false
      }
    }
  }

  def testingStringVsByte(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "byte").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", "100"))) {
      println("testingStringVsByte Succeeded")
      return true
    }
    else {
      println("testingStringVsByte Failed")
      return false
    }
  }

  def testingStringVsShort(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "short").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", "10000"))) {
      println("testingStringVsShort Succeeded")
      return true
    }
    else {
      println("testingStringVsShort Failed")
      return false
    }
  }

  def testingStringVsInt(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "int").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", "5000"))) {
      println("testingStringVsInt Succeeded")
      return true
    }
    else {
      println("testingStringVsInt Failed")
      return false
    }
  }

  def testingStringVsLong(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "long").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", "12345678999"))) {
      println("testingStringVsLong Succeeded")
      return true
    }
    else {
      println("testingStringVsLong Failed")
      return false
    }
  }

  def testingStringVsFloat(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "float").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", "10.1234"))) {
      println("testingStringVsFloat Succeeded")
      return true
    }
    else {
      println("testingStringVsFloat Failed")
      return false
    }
  }

  def testingStringVsDouble(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "double").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", "10.1234567891"))) {
      println("testingStringVsDouble Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).foreach(println)
      println("testingStringVsDouble Failed")
      return false
    }
  }

  def testingStringVsDate(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "date").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", ODate.fromDaysSinceEpoch(1000).toDateStr))) {
      println("testingStringVsDate Succeeded")
      return true
    }
    else {
      println("testingStringVsDate Failed")
      return false
    }
  }

  def testingStringVsTime(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "time").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", OTime.fromMillisOfDay(1000).toString))) {
      println("testingStringVsTime Succeeded")
      return true
    }
    else {
      println("testingStringVsTime Failed")
      return false
    }
  }

  def testingStringVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "timestamp").toDF
    if (booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("string"))).toSet.equals(Set("hello", OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr).toString))) {
      println("testingStringVsTimeStamp Succeeded")
      return true
    }
    else {
      println("testingStringVsTimeStamp Failed")
      return false
    }
  }

  def testingStringVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "binary").toDF.collect
      println("testingStringVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingStringVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingStringVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingStringVsBinary Failed")
        return false
      }
    }
  }

  def testingStringVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "map").toDF.collect
      println("testingStringVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingStringVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingStringVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingStringVsMap Failed")
        return false
      }
    }
  }

  def testingStringVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "string" or field("_id") === "array").toDF.collect
      println("testingStringVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingStringVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingStringVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingStringVsArray Failed")
        return false
      }
    }
  }

  def testingByteVsShort(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "short").toDF
    if (booleanAndNullDF.collect.map(row => row.getShort(row.fieldIndex("byte"))).toSet.equals(Set(100, 10000))) {
      println("testingByteVsShort Succeeded")
      return true
    }
    else {
      println("testingByteVsShort Failed")
      return false
    }
  }

  def testingByteVsInt(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "int").toDF
    if (booleanAndNullDF.collect.map(row => row.getInt(row.fieldIndex("byte"))).toSet.equals(Set(100, 5000))) {
      println("testingByteVsInt Succeeded")
      return true
    }
    else {
      println("testingByteVsInt Failed")
      return false
    }
  }

  def testingByteVsLong(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "long").toDF
    if (booleanAndNullDF.collect.map(row => row.getLong(row.fieldIndex("byte"))).toSet.equals(Set(100L, 12345678999L))) {
      println("testingByteVsLong Succeeded")
      return true
    }
    else {
      println("testingByteVsLong Failed")
      return false
    }
  }

  def testingByteVsFloat(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "float").toDF
    if (booleanAndNullDF.collect.map(row => row.getFloat(row.fieldIndex("byte"))).toSet.equals(Set(100f, 10.1234f))) {
      println("testingByteVsFloat Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getFloat(row.fieldIndex("byte"))).foreach(println)
      println("testingByteVsFloat Failed")
      return false
    }
  }

  def testingByteVsDouble(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "double").toDF
    if (booleanAndNullDF.collect.map(row => row.getDouble(row.fieldIndex("byte"))).toSet.equals(Set(100, 10.1234567891d))) {
      println("testingByteVsDouble Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("byte"))).foreach(println)
      println("testingByteVsDouble Failed")
      return false
    }
  }

  def testingByteVsDate(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "date").toDF.collect
      println("testingByteVsDate Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingByteVsDate Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingByteVsDate Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingByteVsDate Failed")
        return false
      }
    }
  }

  def testingByteVsTime(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "time").toDF.collect
      println("testingByteVsTime Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingByteVsTime Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingByteVsTime Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingByteVsTime Failed")
        return false
      }
    }
  }

  def testingByteVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "timestamp").toDF.collect
      println("testingByteVsTimeStamp Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingByteVsTimestamp Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingByteVsTimestamp Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingByteVsTimeStamp Failed")
        return false
      }
    }
  }

  def testingByteVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "binary").toDF.collect
      println("testingByteVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingByteVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingByteVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingByteVsBinary Failed")
        return false
      }
    }
  }

  def testingByteVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "map").toDF.collect
      println("testingByteVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingByteVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingByteVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingByteVsMap Failed")
        return false
      }
    }
  }

  def testingByteVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "byte" or field("_id") === "array").toDF.collect
      println("testingByteVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingByteVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingByteVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingByteVsArray Failed")
        return false
      }
    }
  }

  def testingShortVsInt(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "int").toDF
    if (booleanAndNullDF.collect.map(row => row.getInt(row.fieldIndex("byte"))).toSet.equals(Set(10000, 5000))) {
      println("testingShortVsInt Succeeded")
      return true
    }
    else {
      println("testingShortVsInt Failed")
      return false
    }
  }

  def testingShortVsLong(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "long").toDF
    if (booleanAndNullDF.collect.map(row => row.getLong(row.fieldIndex("short"))).toSet.equals(Set(10000, 12345678999L))) {
      println("testingShortVsLong Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getLong(row.fieldIndex("short"))).foreach(println)
      println("testingShortVsLong Failed")
      return false
    }
  }

  def testingShortVsFloat(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "float").toDF
    if (booleanAndNullDF.collect.map(row => row.getFloat(row.fieldIndex("short"))).toSet.equals(Set(10000f, 10.1234f))) {
      println("testingShortVsFloat Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getFloat(row.fieldIndex("short"))).foreach(println)
      println("testingShortVsFloat Failed")
      return false
    }
  }

  def testingShortVsDouble(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "double").toDF
    if (booleanAndNullDF.collect.map(row => row.getDouble(row.fieldIndex("short"))).toSet.equals(Set(10000, 10.1234567891d))) {
      println("testingShortVsDouble Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("short"))).foreach(println)
      println("testingShortVsDouble Failed")
      return false
    }
  }

  def testingShortVsDate(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "date").toDF.collect
      println("testingShortVsDate Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingShortVsDate Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingShortVsDate Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingShortVsDate Failed")
        return false
      }
    }
  }

  def testingShortVsTime(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "time").toDF.collect
      println("testingShortVsTime Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingShortVsTime Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingShortVsTime Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingShortVsTime Failed")
        return false
      }
    }
  }

  def testingShortVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "timestamp").toDF.collect
      println("testingShortVsTimeStamp Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingShortVsTimestamp Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingShortVsTimestamp Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingShortVsTimeStamp Failed")
        return false
      }
    }
  }

  def testingShortVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "binary").toDF.collect
      println("testingShortVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingShortVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingShortVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingShortVsBinary Failed")
        return false
      }
    }
  }

  def testingShortVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "map").toDF.collect
      println("testingShortVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingShortVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingShortVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingShortVsMap Failed")
        return false
      }
    }
  }

  def testingShortVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "short" or field("_id") === "array").toDF.collect
      println("testingShortVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingShortVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingShortVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingShortVsArray Failed")
        return false
      }
    }
  }

  def testingIntVsLong(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "long").toDF
    if (booleanAndNullDF.collect.map(row => row.getLong(row.fieldIndex("int"))).toSet.equals(Set(5000L, 12345678999L))) {
      println("testingIntVsLong Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getLong(row.fieldIndex("int"))).foreach(println)
      println("testingIntVsLong Failed")
      return false
    }
  }

  def testingIntVsFloat(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "float").toDF
    if (booleanAndNullDF.collect.map(row => row.getFloat(row.fieldIndex("int"))).toSet.equals(Set(5000f, 10.1234f))) {
      println("testingIntVsFloat Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getFloat(row.fieldIndex("int"))).foreach(println)
      println("testingIntVsFloat Failed")
      return false
    }
  }

  def testingIntVsDouble(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "double").toDF
    if (booleanAndNullDF.collect.map(row => row.getDouble(row.fieldIndex("int"))).toSet.equals(Set(5000d, 10.1234567891d))) {
      println("testingSIntVsDouble Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("int"))).foreach(println)
      println("testingIntVsDouble Failed")
      return false
    }
  }

  def testingIntVsDate(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "date").toDF.collect
      println("testingIntVsDate Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingIntVsDate Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingIntVsDate Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingIntVsDate Failed")
        return false
      }
    }
  }

  def testingIntVsTime(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "time").toDF.collect
      println("testingIntVsTime Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingIntVsTime Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingIntVsTime Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingIntVsTime Failed")
        return false
      }
    }
  }

  def testingIntVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "timestamp").toDF.collect
      println("testingIntVsTimeStamp Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingIntVsTimestamp Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingIntVsTimestamp Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingIntVsTimeStamp Failed")
        return false
      }
    }
  }

  def testingIntVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "binary").toDF.collect
      println("testingIntVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingIntVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingIntVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingIntVsBinary Failed")
        return false
      }
    }
  }

  def testingIntVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "map").toDF.collect
      println("testingIntVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingIntVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingIntVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingShortVsMap Failed")
        return false
      }
    }
  }

  def testingIntVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "int" or field("_id") === "array").toDF.collect
      println("testingIntVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingIntVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingIntVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingIntVsArray Failed")
        return false
      }
    }
  }

  def testingLongVsFloat(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "float").toDF.collect
      println("testingLongVsFloat Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingLongVsFloat Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingLongVsFloat Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingLongVsFloat Failed")
        return false
      }
    }
  }

  def testingLongVsDouble(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "double").toDF
    if (booleanAndNullDF.collect.map(row => row.getDouble(row.fieldIndex("long"))).toSet.equals(Set(12345678999d, 10.1234567891d))) {
      println("testingLongVsDouble Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getString(row.fieldIndex("long"))).foreach(println)
      println("testingLongVsDouble Failed")
      return false
    }
  }

  def testingLongVsDate(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "date").toDF.collect
      println("testingLongVsDate Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingLongVsDate Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingLongVsDate Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingLongVsDate Failed")
        return false
      }
    }
  }

  def testingLongVsTime(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "time").toDF.collect
      println("testingLongVsTime Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingLongVsTime Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingLongVsTime Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingLongVsTime Failed")
        return false
      }
    }
  }

  def testingLongVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "timestamp").toDF.collect
      println("testingLongVsTimeStamp Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingLongVsTimestamp Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingLongVsTimestamp Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingLongVsTimeStamp Failed")
        return false
      }
    }
  }

  def testingLongVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "binary").toDF.collect
      println("testingLongVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingLongVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingLongVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingLongVsBinary Failed")
        return false
      }
    }
  }

  def testingLongVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "map").toDF.collect
      println("testingLongVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingLongVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingLongVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingLongVsMap Failed")
        return false
      }
    }
  }

  def testingLongVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "long" or field("_id") === "array").toDF.collect
      println("testingLongVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingLongVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingLongVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingLongVsArray Failed")
        return false
      }
    }
  }

  def testingFloatVsDouble(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "float" or field("_id") === "double").toDF
    if (booleanAndNullDF.collect.map(row => row.getDouble(row.fieldIndex("float"))).toSet.equals(Set(10.12339973449707d, 10.1234567891d))) {
      println("testingFloatVsDouble Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => row.getDouble(row.fieldIndex("float"))).foreach(println)
      println("testingFloatVsDouble Failed")
      return false
    }
  }

  def testingFloatVsDate(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "float" or field("_id") === "date").toDF.collect
      println("testingFloatVsDate Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingFloatVsDate Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingFloatVsDate Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingFloatVsDate Failed")
        return false
      }
    }
  }

  def testingFloatVsTime(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "float" or field("_id") === "time").toDF.collect
      println("testingFloatVsTime Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingFloatVsTime Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingFloatVsTime Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingFloatVsTime Failed")
        return false
      }
    }
  }

  def testingFloatVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "float" or field("_id") === "timestamp").toDF.collect
      println("testingFloatVsTimeStamp Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingFloatVsTimestamp Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingFloatVsTimestamp Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingFloatVsTimeStamp Failed")
        return false
      }
    }
  }

  def testingFloatVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "float" or field("_id") === "binary").toDF.collect
      println("testingFloatVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingFloatVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingFloatVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingFloatsBinary Failed")
        return false
      }
    }
  }

  def testingFloatVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "float" or field("_id") === "map").toDF.collect
      println("testingFloatVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingFloatVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingFloatVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingFloatVsMap Failed")
        return false
      }
    }
  }

  def testingFloatVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "float" or field("_id") === "array").toDF.collect
      println("testingFloatVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingFloatVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingFloatVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingFloatVsArray Failed")
        return false
      }
    }
  }


  def testingDoubleVsDate(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "double" or field("_id") === "date").toDF.collect
      println("testingDoubleVsDate Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDoubleVsDate Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDoubleVsDate Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDoubleVsDate Failed")
        return false
      }
    }
  }

  def testingDoubleVsTime(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "double" or field("_id") === "time").toDF.collect
      println("testingDoubleVsTime Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDoubleVsTime Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDoubleVsTime Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDoubleVsTime Failed")
        return false
      }
    }
  }

  def testingDoubleVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    try{
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "double" or field("_id") === "timestamp").toDF.collect
      println("testingDoubleVsTimeStamp Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDoubleVsTimestamp Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDoubleVsTimestamp Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDoubleVsTimeStamp Failed")
        return false
      }
    }
  }

  def testingDoubleVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "double" or field("_id") === "binary").toDF.collect
      println("testingDoubleVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDoubleVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDoubleVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDoubleBinary Failed")
        return false
      }
    }
  }

  def testingDoubleVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "double" or field("_id") === "map").toDF.collect
      println("testingDoubleVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDoubleVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDoubleVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDoubleVsMap Failed")
        return false
      }
    }
  }

  def testingDoubleVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "double" or field("_id") === "array").toDF.collect
      println("testingDoubleVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDoubleVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDoubleVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDoubleVsArray Failed")
        return false
      }
    }
  }

  def testingDateVsTime(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "date" or field("_id") === "time").toDF
    if (booleanAndNullDF.collect.map(row => new OTimestamp(row.getTimestamp(row.fieldIndex("time")).getTime)).toSet.equals(
      Set(OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr), new OTimestamp(OTime.fromMillisOfDay(1000).toDate.getTime)))) {
      println("testingDateVsTime Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => new OTimestamp(row.getTimestamp(row.fieldIndex("time")))).foreach(println)
      Set(OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr), new OTimestamp(OTime.fromMillisOfDay(1000).toDate.getTime)).foreach(println)
      println("testingDateVsTime Failed")
      return false
    }
  }

  def testingDateVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "date" or field("_id") === "timestamp").toDF
    if (booleanAndNullDF.collect.map(row => new OTimestamp(row.getTimestamp(row.fieldIndex("timestamp")))).toSet.equals(
      Set(OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr), OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr)))) {
      println("testingDateVsTimeStamp Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => new OTimestamp(row.getTimestamp(row.fieldIndex("timestamp")))).foreach(println)
      Set(OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr), OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr)).foreach(println)
      println("testingDateVsTimeStamp Failed")
      return false
    }
  }

  def testingDateVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "date" or field("_id") === "binary").toDF.collect
      println("testingDateVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDateVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDateVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDateBinary Failed")
        return false
      }
    }
  }

  def testingDateVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "date" or field("_id") === "map").toDF.collect
      println("testingDateVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDateVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDateVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDateVsMap Failed")
        return false
      }
    }
  }

  def testingDateVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "date" or field("_id") === "array").toDF.collect
      println("testingDateVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingDateVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingDateVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingDateVsArray Failed")
        return false
      }
    }
  }

  def testingTimeVsTimeStamp(spark: SparkSession, tableName: String): Boolean = {
    val booleanAndNullDF = spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "time" or field("_id") === "timestamp").toDF
    if (booleanAndNullDF.collect.map(row => new OTimestamp(row.getTimestamp(row.fieldIndex("time")))).toSet.equals(
      Set(OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr), new OTimestamp(OTime.fromMillisOfDay(1000).toDate.getTime)))) {
      println("testingTimeVsTimeStamp Succeeded")
      return true
    }
    else {
      booleanAndNullDF.collect.map(row => new OTimestamp(row.getTimestamp(row.fieldIndex("time")))).foreach(println)
      //Set(OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr), OTimestamp.parse(ODate.fromDaysSinceEpoch(1000).toDateStr)).foreach(println)
      println("testingTimeVsTimeStamp Failed")
      return false
    }
  }

  def testingTimeVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "time" or field("_id") === "binary").toDF.collect
      println("testingTimeVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingTimeVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingTimeVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingTimeBinary Failed")
        return false
      }
    }
  }

  def testingTimeVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "time" or field("_id") === "map").toDF.collect
      println("testingTimeVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingTimeVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingTimeVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingTimeVsMap Failed")
        return false
      }
    }
  }

  def testingTimeVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "time" or field("_id") === "array").toDF.collect
      println("testingTimeVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingTimeVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingTimeVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingTimeVsArray Failed")
        return false
      }
    }
  }

  def testingTimeStampVsBinary(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "timestamp" or field("_id") === "binary").toDF.collect
      println("testingTimeStampVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingTimeStampVsBinary Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingTimeStampVsBinary Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingTimeStampBinary Failed")
        return false
      }
    }
  }

  def testingTimeStampVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "timestamp" or field("_id") === "map").toDF.collect
      println("testingTimeStampVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingTimeStampVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingTimeStampVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingTimeStampVsMap Failed")
        return false
      }
    }
  }

  def testingTimeStampVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "timestamp" or field("_id") === "array").toDF.collect
      println("testingTimeStampVsBinary Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingTimeStampVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingTimeStampVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingTimeStampVsArray Failed")
        return false
      }
    }
  }

  def testingBinaryVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "binary" or field("_id") === "map").toDF.collect
      println("testingBinaryVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBinaryVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBinaryVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBinaryVsMap Failed")
        return false
      }
    }
  }

  def testingBinaryVsArray(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "binary" or field("_id") === "array").toDF.collect
      println("testingBinaryVsArray Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingBinaryVsArray Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingBinaryVsArray Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingBinaryVsArray Failed")
        return false
      }
    }
  }

  def testingArrayVsMap(spark: SparkSession, tableName: String): Boolean = {
    try {
      spark.sparkContext.loadFromMapRDB(tableName).where(field("_id") === "array" or field("_id") === "map").toDF.collect
      println("testingArrayVsMap Failed")
      return false
    } catch {
      case ex: SparkException => {
        ex.getCause match {
          case ex: SchemaMappingException => {
            println("testingArrayVsMap Succeeded")
            return true
          }
          case ex: Throwable => {
            println("testingArrayVsMap Failed")
            return false
          }
        }
      }
      case ex: Throwable => {
        println("testingArrayVsMap Failed")
        return false
      }
    }
  }

    def testingCustomer(spark: SparkSession, tableName: String): Boolean = {
        spark.sqlContext.read.format("com.mapr.db.spark.sql.DefaultSource").option("tableName", "/tmp/customer").load().select("features").show(10)
        println("testingCustomer Failed")
        return true
    }

  def runTests(sparkSession: SparkSession): Unit = {
    testingBooleanVsNull(sparkSession, tableName)
    testingStringVsNull(sparkSession, tableName)
    testingByteVsNull(sparkSession, tableName)
    testingShortVsNull(sparkSession, tableName)
    testingIntVsNull(sparkSession, tableName)
    testingLongVsNull(sparkSession, tableName)
    testingFloatVsNull(sparkSession, tableName)
    testingDoubleVsNull(sparkSession, tableName)
    testingDateVsNull(sparkSession, tableName)
    testingTimeVsNull(sparkSession, tableName)
    testingTimeStampVsNull(sparkSession, tableName)
    testingBinaryVsNull(sparkSession, tableName)
    testingMapVsNull(sparkSession, tableName)
    testingArrayVsNull(sparkSession, tableName)
    testingBooleanVsString(sparkSession, tableName)
    testingBooleanVsByte(sparkSession, tableName)
    testingBooleanVsShort(sparkSession, tableName)
    testingBooleanVsInt(sparkSession, tableName)
    testingBooleanVsLong(sparkSession, tableName)
    testingBooleanVsFloat(sparkSession, tableName)
    testingBooleanVsDouble(sparkSession, tableName)
    testingBooleanVsDate(sparkSession, tableName)
    testingBooleanVsTime(sparkSession, tableName)
    testingBooleanVsTimeStamp(sparkSession, tableName)
    testingBooleanVsBinary(sparkSession, tableName)
    testingBooleanVsMap(sparkSession, tableName)
    testingBooleanVsArray(sparkSession, tableName)
    testingStringVsByte(sparkSession, tableName)
    testingStringVsShort(sparkSession, tableName)
    testingStringVsInt(sparkSession, tableName)
    testingStringVsLong(sparkSession, tableName)
    testingStringVsFloat(sparkSession, tableName)
    testingStringVsDouble(sparkSession, tableName)
    testingStringVsDate(sparkSession, tableName)
    testingStringVsTime(sparkSession, tableName)
    testingStringVsTimeStamp(sparkSession, tableName)
    testingStringVsBinary(sparkSession, tableName)
    testingStringVsMap(sparkSession, tableName)
    testingStringVsArray(sparkSession, tableName)
    testingByteVsShort(sparkSession, tableName)
    testingByteVsInt(sparkSession, tableName)
    testingByteVsLong(sparkSession, tableName)
    testingByteVsFloat(sparkSession, tableName)
    testingByteVsLong(sparkSession, tableName)
    testingByteVsFloat(sparkSession, tableName)
    testingByteVsDouble(sparkSession, tableName)
    testingByteVsDate(sparkSession, tableName)
    testingByteVsTime(sparkSession, tableName)
    testingByteVsTimeStamp(sparkSession, tableName)
    testingByteVsBinary(sparkSession, tableName)
    testingByteVsMap(sparkSession, tableName)
    testingByteVsArray(sparkSession, tableName)
    testingShortVsInt(sparkSession, tableName)
    testingShortVsLong(sparkSession, tableName)
    testingShortVsFloat(sparkSession, tableName)
    testingShortVsLong(sparkSession, tableName)
    testingShortVsFloat(sparkSession, tableName)
    testingShortVsDouble(sparkSession, tableName)
    testingShortVsDate(sparkSession, tableName)
    testingShortVsTime(sparkSession, tableName)
    testingShortVsTimeStamp(sparkSession, tableName)
    testingShortVsBinary(sparkSession, tableName)
    testingShortVsMap(sparkSession, tableName)
    testingShortVsArray(sparkSession, tableName)
    testingIntVsLong(sparkSession, tableName)
    testingIntVsFloat(sparkSession, tableName)
    testingIntVsDouble(sparkSession, tableName)
    testingIntVsDate(sparkSession, tableName)
    testingIntVsTime(sparkSession, tableName)
    testingIntVsTimeStamp(sparkSession, tableName)
    testingIntVsBinary(sparkSession, tableName)
    testingIntVsMap(sparkSession, tableName)
    testingIntVsArray(sparkSession, tableName)
    testingLongVsFloat(sparkSession, tableName)
    testingLongVsDouble(sparkSession, tableName)
    testingLongVsDate(sparkSession, tableName)
    testingLongVsTime(sparkSession, tableName)
    testingLongVsTimeStamp(sparkSession, tableName)
    testingLongVsBinary(sparkSession, tableName)
    testingLongVsMap(sparkSession, tableName)
    testingLongVsArray(sparkSession, tableName)
    testingFloatVsDouble(sparkSession, tableName)
    testingFloatVsDate(sparkSession, tableName)
    testingFloatVsTime(sparkSession, tableName)
    testingFloatVsTimeStamp(sparkSession, tableName)
    testingFloatVsBinary(sparkSession, tableName)
    testingFloatVsMap(sparkSession, tableName)
    testingFloatVsArray(sparkSession, tableName)
    testingDoubleVsDate(sparkSession, tableName)
    testingDoubleVsTime(sparkSession, tableName)
    testingDoubleVsTimeStamp(sparkSession, tableName)
    testingDoubleVsBinary(sparkSession, tableName)
    testingDoubleVsMap(sparkSession, tableName)
    testingDoubleVsArray(sparkSession, tableName)
    testingDateVsTime(sparkSession, tableName)
    testingDateVsTimeStamp(sparkSession, tableName)
    testingDateVsBinary(sparkSession, tableName)
    testingDateVsArray(sparkSession, tableName)
    testingDateVsMap(sparkSession, tableName)
    testingTimeVsTimeStamp(sparkSession, tableName)
    testingTimeVsBinary(sparkSession, tableName)
    testingTimeVsMap(sparkSession, tableName)
    testingTimeVsArray(sparkSession, tableName)
    testingTimeStampVsBinary(sparkSession, tableName)
    testingTimeStampVsMap(sparkSession, tableName)
    testingTimeStampVsArray(sparkSession, tableName)
    testingBinaryVsArray(sparkSession, tableName)
    testingBinaryVsMap(sparkSession, tableName)
    testingArrayVsMap(sparkSession, tableName)
  }
}

object SparkSqlAccessTestsWithKryo {
  val tableName="/tmp/SparkSqlOjaiConnectorAccessTesting"
  lazy val conf = new SparkConf()
    .setAppName("SparkSqlAccessTestsWithKryo")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")

  lazy val spark = SparkSession.builder().appName("SparkSqlAccessTestsWithKryo").config(conf).getOrCreate()


  def main(args: Array[String]): Unit = {
    SparkSqlAccessTests.tableInitialization(tableName)
    SparkSqlAccessTests.runTests(spark)
  }
}
