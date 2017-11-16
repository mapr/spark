/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.testCases

import java.util

import org.apache.spark.rdd.RDD
import java.nio.ByteBuffer
import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.types.DBBinaryValue
import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.rowcol.DBDocumentImpl
import org.ojai.types._


object OjaiDocumentAccessTesting {
  lazy val conf = new SparkConf()
    .setAppName("simpletest")
    .set("spark.executor.memory","1g")
    .set("spark.driver.memory", "1g")

  def main(args: Array[String]): Unit = {
    var sc = new SparkContext(conf)
    runTests(sc)
  }

  def runTests(sparkSession: SparkContext): Unit = {
    testGetterFuncForInt(sparkSession)
    testGetterFuncForByte(sparkSession)
    testGetterFuncForString(sparkSession)
    testGetterFuncForShort(sparkSession)
    testGetterFuncForLong(sparkSession)
    testGetterFuncForFloat(sparkSession)
    testGetterFuncForDouble(sparkSession)
    testGetterFuncForTime(sparkSession)
    testGetterFuncForDate(sparkSession)
    testGetterFuncForTimeStamp(sparkSession)
    testGetterFuncForBinary(sparkSession)
    testGetterFuncForList(sparkSession)
    testGetterFuncForMap(sparkSession)
    testGetterFuncForIntExpl(sparkSession)
    testGetterFuncForByteExpl(sparkSession)
    testGetterFuncForStringExpl(sparkSession)
    testGetterFuncForShortExpl(sparkSession)
    testGetterFuncForLongExpl(sparkSession)
    testGetterFuncForFloatExpl(sparkSession)
    testGetterFuncForDoubleExpl(sparkSession)
    testGetterFuncForTimeExpl(sparkSession)
    testGetterFuncForDateExpl(sparkSession)
    testGetterFuncForTimeStampExpl(sparkSession)
    testGetterFuncForBinaryExpl(sparkSession)
    testGetterFuncForArrayExpl(sparkSession)
    testGetterFuncForMapExpl(sparkSession)
    testSetterFuncForInt(sparkSession)
    testSetterFuncForByte(sparkSession)
    testSetterFuncForString(sparkSession)
    testSetterFuncForShort(sparkSession)
    testSetterFuncForLong(sparkSession)
    testSetterFuncForFloat(sparkSession)
    testSetterFuncForDouble(sparkSession)
    testSetterFuncForTime(sparkSession)
    testSetterFuncForDate(sparkSession)
    testSetterFuncForTimeStamp(sparkSession)
    testSetterFuncForBinary(sparkSession)
    testSetterFuncForBinaryWithByteArr(sparkSession)
    testSetterFuncForList(sparkSession)
    testSetterFuncForMap(sparkSession)
    testGetterNoDataCase(sparkSession)
    testSetterNullToDoc(sparkSession)
    testSetterFuncForMapStringInt(sparkSession)
    testNonDynamicSetterFuncForInt(sparkSession)
  }

  private def createOJAIDocumentsRDD(sc: SparkContext) = {
    val rec: DBDocumentImpl = new DBDocumentImpl()
    rec.setId(1233456.toString)
    rec.set("map.field1", 100.toByte)
    rec.set("map.field2", 10000.toShort)
    rec.set("map.longfield2verylongverylong", 12.345678)
    rec.set("FIELD2", "VERY LONG STRING IS THIS YOU KNOW")

    rec.set("map2.field1", 100.toByte)
    rec.set("map2.field2", 10000.toShort)
    rec.set("map2.longfield2verylongverylong", 12.345678)
    rec.set("FIELD3", "VERY LONG STRING IS THIS YOU KNOW")
    val map: util.Map [java.lang.String, Object] = new util.HashMap [java.lang.String, Object]()
    map.put("Name", "Aditya")
    map.put("Age", new Integer(20))
    rec.set("map.map", map)
    rec.set("map.boolean", false)
    rec.set("map.string", "string")
    rec.set("map.byte", 100.toByte)
    rec.set("map.short", 10000.toShort)
    rec.set("map.int", new Integer(5000))
    rec.set("map.long", 12345678999L)
    rec.set("map.float", 10.1234f)
    rec.set("map.double", 10.12345678910d)
    //    rec.set("map.interval", new OInterval(1000))
    rec.set("map.time", new OTime(1000))
    rec.set("map.date", new ODate(1000))
    rec.set("map.timestamp", new OTimestamp(1000))
    //    rec.set("map.decimal", new java.math.BigDecimal("1000000000.11111111111111111111"))

    val bytes: Array[Byte] = Array.range(1, 10).map(_.toByte)
    rec.set("map.binary1", bytes)
    rec.set("map.binary2", bytes, 1, 3)
    val bbuf: ByteBuffer = ByteBuffer.allocate(100)
    (1 to 100).foldLeft[ByteBuffer](bbuf)((buf, a) => buf.put(a.toByte))
    rec.set("map.binary3", bbuf)

    val values = new util.ArrayList[Object]()
    values.add("Field1")
    val intvalue: Integer = 500
    values.add(intvalue)
    values.add(new java.lang.Double(5555.5555))
    rec.set("map.list", values)
    val idstring : String = rec.getIdAsString
    val doc = MapRDBSpark.newDocument(rec)
    val ojairdd = sc.parallelize(List(doc))
    ojairdd
  }

  /* All the following functions will test the functionality of accessing the data with corresponding data type from a ojai document*/
  def testGetterFuncForInt(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.int`).collect
    if (result.sameElements(Array(5000)))
      {
        println("testGetterFuncForInt succeeded")
        true
      }
    else
      {
        println("testGetterFuncForInt failed")
        false
      }
  }

  def testGetterFuncForByte(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.byte`).collect
    if (result.sameElements(Array(100.toByte)))
      {
        println("testGetterFuncForByte succeeded")
        true
      }
    else
      {
        println("testGetterFuncForByte failed")
        false
      }
  }

  def testGetterFuncForString(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.string`).collect
    if (result.sameElements(Array("string")))
      {
        println("testGetterFuncForString succeeded")
        true
      }
    else
      {
        println("testGetterFuncForString falied")
        false
      }
  }

  def testGetterFuncForShort(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.short`).collect
    if (result.sameElements(Array(10000.toShort)))
      {
        println("testGetterFuncForShort succeeded")
        true
      }
    else
      {
        println("testGetterFuncForShort falied")
        false
      }
  }

  def testGetterFuncForLong(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.long`).collect
    if (result.sameElements(Array(12345678999L)))
      {
        println("testGetterFuncForLong succeeded")
        true
      }
    else
      {
        println("testGetterFuncForLong falied")
        false
      }
  }

  def testGetterFuncForFloat(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.float`).collect
    if (result.sameElements(Array(10.1234f)))
      {
        println("testGetterFuncForFloat succeeded")
        true
      }
    else
      {
        println("testGetterFuncForFloat failed")
        false
      }
  }

  def testGetterFuncForDouble(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.double`).collect
    if (result.sameElements(Array(10.12345678910d)))
      {
        println("testGetterFuncForDouble succeeded")
        true
      }
    else
      {
        println("testGetterFuncForDouble failed")
        false
      }
  }

  def testGetterFuncForTime(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.time`).collect
    if (result.sameElements(Array(new OTime(1000))))
      {
        println("testGetterFuncForTime succeeded")
        true
      }
    else {
      println("testGetterFuncForTime failed")
      false
    }
  }

  def testGetterFuncForDate(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.date`).collect
    if (result.sameElements(Array(new ODate(1000))))
      {
        println("testGetterFuncForDate succeeded")
        true
      }
    else {
      println("testGetterFuncForDate failed")
      false
    }
  }

  def testGetterFuncForTimeStamp(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.timestamp`).collect
    if (result.sameElements(Array(new OTimestamp(1000))))
      {
        println("testGetterFuncForTimeStamp succeeded")
        true
      }
    else {
      println("testGetterFuncForTimeStamp failed")
      false
    }
  }

  def testGetterFuncForBinary(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.getBinarySerializable("map.binary1")).collect
    if (result.map(a => a.array().toSet).sameElements(Array(Set(1, 2, 3, 4, 5, 6, 7, 8, 9))))
      {
        println("testGetterFuncForBinary succeeded")
        true
      }
    else {
      println("testGetterFuncForBinary failed")
      false
    }
  }

  def testGetterFuncForList(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.list`).collect
    if (result.map(a => a.asInstanceOf[Seq[Any]].toSet).sameElements(Array(Set("Field1",500,5555.5555))))
      {
        println("testGetterFuncForList succeeded")
        true
      }
    else {
      println("testGetterFuncForList failed")
      false
    }
  }

  def testGetterFuncForMap(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.map`).collect
    if (result.map(a => a.asInstanceOf[Map[String,Any]].toSet).sameElements(Array(Set("Age" -> 20, "Name" -> "Aditya"))))
      {
        println("testGetterFuncForMap succeeded")
        true
      }
    else {
      println("testGetterFuncForMap failed")
      false
    }
  }



  /* getters with explicit type casts*/

  def testGetterFuncForIntExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.int`[Integer]).collect
    if (result.sameElements(Array(5000)))
      {
        println("testGetterFuncForIntExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForIntExpl failed")
      false
    }
  }

  def testGetterFuncForByteExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.byte`[Byte]).collect
    if (result.sameElements(Array(100.toByte)))
      {
        println("testGetterFuncForByteExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForByteExpl failed")
      false
    }
  }

  def testGetterFuncForStringExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.string`[String]).collect
    if (result.sameElements(Array("string")))
      {
        println("testGetterFuncForStringExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForStringExpl failed")
      false
    }
  }

  def testGetterFuncForShortExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.short`[Short]).collect
    if (result.sameElements(Array(10000.toShort)))
      {
        println("testGetterFuncForShortExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForShortExpl failed")
      false
    }
  }

  def testGetterFuncForLongExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.long`[Long]).collect
    if (result.sameElements(Array(12345678999L)))
      {
        println("testGetterFuncForLongExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForLongExpl failed")
      false
    }
  }

  def testGetterFuncForFloatExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.float`[Float]).collect
    if (result.sameElements(Array(10.1234f)))
      {
        println("testGetterFuncForFloatExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForFloatExpl failed")
      false
    }
  }

  def testGetterFuncForDoubleExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.double`[Double]).collect
    if (result.sameElements(Array(10.12345678910d)))
      {
        println("testGetterFuncForDoubleExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForDoubleExpl failed")
      false
    }
  }

  def testGetterFuncForTimeExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.time`[OTime]).collect
    if (result.sameElements(Array(new OTime(1000))))
      {
        println("testGetterFuncForTimeExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForTimeExpl failed")
      false
    }
  }

  def testGetterFuncForDateExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.date`[ODate]).collect
    if (result.sameElements(Array(new ODate(1000))))
      {
        println("testGetterFuncForDateExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForDateExpl failed")
      false
    }
  }

  def testGetterFuncForTimeStampExpl(sparkSession : SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.timestamp`[OTimestamp]).collect
    if (result.sameElements(Array(new OTimestamp(1000))))
      {
        println("testGetterFuncForTimeStampExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForTimeStampExpl failed")
      false
    }
  }

  def testGetterFuncForBinaryExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.getBinarySerializable("map.binary1")).collect
    if (result.map(a => a.array().toSet).sameElements(Array(Set(1, 2, 3, 4, 5, 6, 7, 8, 9))))
      {
        println("testGetterFuncForBinaryExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForBinaryExpl failed")
      false
    }
  }

  def testGetterFuncForArrayExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.list`[Seq[Any]]).collect
    if (result.map(a => a.toSet).sameElements(Array(Set("Field1",500,5555.5555))))
      {
        println("testGetterFuncForArrayExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForArrayExpl failed")
      false
    }
  }

  def testGetterFuncForMapExpl(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`map.map`[Map[String,AnyRef]]).collect
    if (result.map(a => a.toSet).sameElements(Array(Set("Age" -> 20, "Name" -> "Aditya"))))
      {
        println("testGetterFuncForMapExpl succeeded")
        true
      }
    else {
      println("testGetterFuncForMapExpl failed")
      false
    }
  }

  /* testing the setters functionality for the ojai document*/
  def testSetterFuncForInt(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.int_new` = a.`map.int`; a } ).map(a => a.`map.int_new`).collect
    if (result.sameElements(Array(5000)))
      {
        println("testSetterFuncForInt succeeded")
        true
      }
    else {
      println("testSetterFuncForInt failed")
      false
    }
  }

  def testSetterFuncForByte(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.byte_new` = a.`map.byte`; a}).map(a => a.`map.byte_new`).collect
    if (result.sameElements(Array(100.toByte)))
      {
        println("testSetterFuncForByte succeeded")
        true
      }
    else {
      println("testSetterFuncForByte failed")
      false
    }
  }

  def testSetterFuncForString(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.string_new` = a.`map.string`; a}).map(a => a.`map.string_new`).collect
    if (result.sameElements(Array("string")))
      {
        println("testSetterFuncForString succeeded")
        true
      }
    else {
      println("testSetterFuncForString failed")
      false
    }
  }

  def testSetterFuncForShort(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => {a.`map.short_new` = a.`map.short`; a}).map(a => a.`map.short_new`).collect
    if (result.sameElements(Array(10000.toShort)))
      {
        println("testSetterFuncForShort succeeded")
        true
      }
    else {
      println("testSetterFuncForShort failed")
      false
    }
  }

  def testSetterFuncForLong(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.long_new` = a.`map.long`; a}).map(a => a.`map.long_new`).collect
    if (result.sameElements(Array(12345678999L)))
      {
        println("testSetterFuncForLong succeeded")
        true
      }
    else {
      println("testSetterFuncForLong failed")
      false
    }
  }

  def testSetterFuncForFloat(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.float_new` = a.`map.float`; a}).map(a => a.`map.float_new`).collect
    if (result.sameElements(Array(10.1234f)))
      {
        println("testSetterFuncForFloat succeeded")
        true
      }
    else {
      println("testSetterFuncForFloat failed")
      false
    }
  }

  def testSetterFuncForDouble(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => {a.`map.double_new` = a.`map.double`; a}).map(a => a.`map.double_new`).collect
    if (result.sameElements(Array(10.12345678910d)))
      {
        println("testSetterFuncForDouble succeeded")
        true
      }
    else {
      println("testSetterFuncForDouble failed")
      false
    }
  }

  def testSetterFuncForTime(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => {a.`map.time_new` = a.`map.time`; a}).map(a => a.`map.time_new`).collect
    if (result.sameElements(Array(new OTime(1000))))
      {
        println("testSetterFuncForTime succeeded")
        true
      }
    else {
      println("testSetterFuncForTime failed")
      false
    }
  }

  def testSetterFuncForDate(sparkSession : SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.date_new` = a.`map.date`; a}).map(a => a.`map.date_new`).collect
    if (result.sameElements(Array(new ODate(1000))))
      {
        println("testSetterFuncForDate succeeded")
        true
      }
    else {
      println("testSetterFuncForDate failed")
      false
    }
  }

  def testSetterFuncForTimeStamp(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.timestamp_new` = a.`map.timestamp`; a}).map(a => a.`map.timestamp_new`).collect
    if (result.sameElements(Array(new OTimestamp(1000))))
      {
        println("testSetterFuncForTimeStamp succeeded")
        true
      }
    else {
      println("testSetterFuncForTimeStamp failed")
      false
    }
  }

  def testSetterFuncForBinary(sparkSession:SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val bbuf: ByteBuffer = ByteBuffer.allocate(100)
    (1 to 100).foldLeft[ByteBuffer](bbuf)((buf, a) => buf.put(a.toByte))
    val result = ojaiDocumentRDD.map(a => { a.`map.binary1_new` = a.getBinarySerializable("map.binary1"); a}).map(a => a.getBinarySerializable("map.binary1_new")).collect
    if (result.map(a => a.array().toSet).sameElements(Array(Set(1, 2, 3, 4, 5, 6, 7, 8, 9))))
      {
        println("testSetterFuncForBinary succeeded")
        true
      }
    else {
      println("testSetterFuncForBinary failed")
      false
    }
  }

  def testSetterFuncForBinaryWithByteArr(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val bbuf: ByteBuffer = ByteBuffer.allocate(100)
    (1 to 100).foldLeft[ByteBuffer](bbuf)((buf, a) => buf.put(a.toByte))
    val result = ojaiDocumentRDD.map(a => { a.`map.binary1_new` = a.getBinarySerializable("map.binary1").array(); a}).map(a => a.getBinarySerializable("map.binary1_new")).collect
    if (result.map(a => a.array().toSet).sameElements(Array(Set(1, 2, 3, 4, 5, 6, 7, 8, 9))))
      {
        println("testSetterFuncForBinaryWithByteArr succeeded")
        true
      }
    else {
      println("testSetterFuncForBinaryWithByteArr failed")
      false
    }
  }

  def testSetterFuncForList(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.list_new` = Seq(1,2,3,4); a}).map(a => a.`map.list_new`).collect
    if (result.map(a => a.asInstanceOf[Seq[Any]]).sameElements(Array(List(1,2,3,4))))
      {
        println("testSetterFuncForList succeeded")
        true
      }
    else {
      println("testSetterFuncForList failed")
      false
    }
  }

  def testSetterFuncForMap(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.map_new` = Map("one" -> 1, "two" -> 2); a}).map(a => a.`map.map_new`).collect
    if (result.map(a => a.asInstanceOf[Map[String,Any]].toSet).sameElements(Array(Set("one" -> 1, "two" -> 2))))
      {
        println("testSetterFuncForMap succeeded")
        true
      }
    else {
      println("testSetterFuncForMap failed")
      false
    }
  }

  /* negative testcases where there exists no data for a particular column in the ojai document*/
  def testGetterNoDataCase(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.`getter`).collect
    if (result.sameElements(Array(null)))
      {
        println("testGetterNoDataCase succeeded")
        true
      }
    else {
      println("testGetterNoDataCase failed")
      false
    }
  }

  /* setting a null value to the document*/
  def testSetterNullToDoc(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.double_latest` = null;a }).map(a => a.`map.double_latest`).collect()
    if (result.sameElements(Array(null)))
      {
        println("testSetterNullToDoc succeeded")
        true
      }
    else {
      println("testSetterNullToDoc failed")
      false
    }
  }

  /* testing map setter functionality extensively*/
  def testSetterFuncForMapStringInt(sparkSession:SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => { a.`map.map_latest` = Map("India" -> "Delhi", "USA" -> "DC", "Germany" -> "Berlin"); a}).map(a => a.`map.map_latest`).collect()
    if (result.map(a => a.asInstanceOf[Map[String,AnyRef]].toSet).sameElements(Array(Set("India" -> "Delhi", "USA" -> "DC", "Germany" -> "Berlin"))))
      {
        println("testSetterFuncForMapStringInt succeeded")
        true
      }
    else {
      println("testSetterFuncForMapStringInt failed")
      false
    }
  }

  /* These tests are with new setter functionality*/
  def testNonDynamicSetterFuncForInt(sparkSession: SparkContext) = {
    val ojaiDocumentRDD = createOJAIDocumentsRDD(sparkSession)
    val result = ojaiDocumentRDD.map(a => a.set("map.int_new", a.`map.int`[Int]) ).map(a => a.`map.int_new`).collect
    if (result.sameElements(Array(5000)))
    {
      println("testSetterFuncForInt succeeded")
      true
    }
    else {
      println("testSetterFuncForInt failed")
      false
    }
  }
}


object OjaiDocumentAccessTestingWithKryo {
  lazy val conf = new SparkConf()
    .setAppName("simpletest")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")

  def main(args: Array[String]): Unit = {
    var sc = new SparkContext(conf)
    OjaiDocumentAccessTesting.runTests(sc)
  }
}
