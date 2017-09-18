/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.testCases

import org.ojai.types.ODate
import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.mapr.db.MapRDB
import com.mapr.db.spark.field
import com.mapr.db.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.ojai.FieldPath
import com.mapr.db.MapRDB
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.testCases.BeanTest.User

object MapRDBSparkTests {
  val tableName = "/tmp/user_profiles_temp"
  val saveToTable = "/tmp/user_profiles_temp_save"
  lazy val conf = new SparkConf()
    .setAppName("simpletest")
    .set("spark.executor.memory","1g")
    .set("spark.driver.memory", "1g")
      .setMaster("local[*]")
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext(conf)
    tableInitialization(sc, tableName)
    runTests(sc)
  }

  def tableInitialization(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists(tableName))
      MapRDB.deleteTable(tableName)
    val docs = scala.collection.immutable.List("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\"," +
      "\"first_name\":\"Robert\"," +
      "\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
      "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}",
      "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
      "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
      "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
    val docsrdd = sparkSession.parallelize(docs)
    val ojairdd = docsrdd.map(doc => MapRDBSpark.newDocument(doc)).map(doc => {
      doc.dob = Option(doc.dob) match {
        case Some(a) => ODate.parse(doc.dob[String])
        case None => null
      }
      doc
    })
    ojairdd.saveToMapRDB(tableName, createTable = true)
  }

  def runTests(sparkSession: SparkContext): Unit = {
    testEmptySet(sparkSession, tableName)
    testShakespearesFamousQuote(sparkSession, tableName)
    testBeanClassParsing(sparkSession, tableName)
    testIntegerZipCodes(sparkSession, tableName)
    testingMapRDBBeanClass(sparkSession, tableName)
    testMaprDBBeanClassWithCollect(sparkSession, tableName)
    testGroupByOnODate(sparkSession, tableName)
    testingLoadingOfOJAIDocuments(sparkSession, tableName)
    testingCoGroupWithOJAIDOCUMENT(sparkSession, tableName)
    testingArrayValueWithOJAIDocument(sparkSession, tableName)
    testingCoGroupArrayKey(sparkSession, tableName)
    testingCoGroupOnMapValue(sparkSession, tableName)
    testingCoGroupOnMapKey(sparkSession, tableName)
    testingCoGroupWithMapKeyWithFilter(sparkSession, tableName)
    testingAssignmentOfZipCodeToOJAIDocument(sparkSession, tableName)
    testingAccessOfFieldsOfOJAIDocumentWithParametricTypes(sparkSession, tableName)
    testingAccessOfFieldsOfOJAIDocumentWithParametricTypes2(sparkSession, tableName)
    testingAccessOfProjectedFields(sparkSession, tableName)
    testingAccessOfProjectedFieldPaths(sparkSession, tableName)
    testingOfSavingTheProcessedOJAIDocuments(sparkSession, tableName)
    testingMapAsaValue(sparkSession, tableName)
    testingMapAsaKey(sparkSession, tableName)
    testingArrayAsaValue(sparkSession, tableName)
    testingArrayAsaKey(sparkSession, tableName)
    testingOJAIDocumentParsingFunctionality(sparkSession, tableName)
    testingMultipleDataTypesInOJAIDocument(sparkSession, tableName)
//    testingMultipleDataTypesInOJAIDocumentAndTypeCasting(sparkSession, tableName)
    testingSingleDataTypeDoubleInOJAIAndTypeCasting(sparkSession, tableName)
    testingTupleOutputOfAnRDD(sparkSession, tableName)
    testingAddingCountryInAddressField(sparkSession, tableName)
    testingBinaryDataTypeInOJAIDocument(sparkSession, tableName)
    testingMapTypeInOJAIDocument(sparkSession, tableName)
    testingDateTypeInOJAIDocument(sparkSession, tableName)
    testingSaveWithAnyObject(sparkSession, tableName)
    testingFilterFunctionOnMapObject(sparkSession, tableName)
    testingFilterFunctionOnArrayObject(sparkSession, tableName)
    testingFilterFunctionOnArrayObjectFunctionalway(sparkSession, tableName)
    testingWhereClauseOnloadFromMapRDB(sparkSession, tableName)
    testingPartitionOnloadFromMapRDB(sparkSession, tableName)
    testingAssignmentOfDocument(sparkSession, tableName)
    testCaseWithFlatMap(sparkSession, tableName)
    testingBulkJoinWithRDD(sparkSession, tableName)
    testingJoinWithRDDBean(sparkSession, tableName)
    testingJoinWithRDD(sparkSession, tableName)
    testingJoinWithOjaiRDDBean(sparkSession, tableName)
    testingUpdateMapRDBTable(sparkSession, tableName)
    }

  def testEmptySet(sparkSession: SparkContext, tableName: String) = {
    val lines = Array("")
    val wordCounts :Seq[WordCount] = WordCount.count(sparkSession, sparkSession.parallelize(lines)).collect()
    if (wordCounts.equals(Seq.empty)) {
      println("testEmptySet succeeded")
      true
    }
    else {
      println("testEmptySet failed")
      false
    }
  }

  def testShakespearesFamousQuote(sparkSession: SparkContext, tableName: String) = {
    val lines = Seq("To be or not to be.", "That is the question.")
    val stopWords = Set("the")
    val wordCounts = WordCount.count(sparkSession, sparkSession.parallelize(lines), stopWords).collect()
    if (wordCounts.sameElements(Array(
      WordCount("be", 2),
      WordCount("is", 1),
      WordCount("not", 1),
      WordCount("or", 1),
      WordCount("question", 1),
      WordCount("that", 1),
      WordCount("to", 2)))) {
      println("testShakespearesFamousQuote succeded")
      true
    }
    else {
      println("testShakespearesFamousQuote failed")
      false
    }
  }

  def testBeanClassParsing(sparkSession: SparkContext, tableName: String) = {

    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    import java.io.StringWriter

    val person = BeanTest.person("fred", 25)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val out = new StringWriter
    mapper.writeValue(out, person)

    val json = out.toString

    val person2 = mapper.readValue[BeanTest.person](json, classOf[BeanTest.person])
    if (person.equals(person2)) {
      println("testBeanClassParsing succeded")
      true
    }
    else {
      println("testBeanClassParsing failed")
      false
    }
  }

  def testIntegerZipCodes(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd.map(a => a.`address.zip`[Integer]).collect
    if (collection.toSet.equals(Set(94105, 95109, null, null, null))) {
      println("testIntegerZipCodes succeded")
      true
    }
    else {
      println("testIntegerZipCodes failed")
      false
    }
  }

  def testingMapRDBBeanClass(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB[BeanTest.User](tableName)
    val collection = maprd.collect()
    if (collection.map(a => a.toString).toSet.equals(Set("User(rsmith,Robert,Smith,1982-02-03,List(electronics, music, sports))",
      "User(mdupont,Maxime,Dupont,1982-02-03,List(sports, movies, electronics))",
      "User(jdoe,John,Doe,1970-06-23,null)",
      "User(dsimon,David,Simon,1980-10-13,null)",
      "User(alehmann,Andrew,Lehmann,1980-10-13,List(html, css, js))")))
      {
        println("testingMapRDBBeanClass succeded")
        true
      }
    else {
      println("testingMapRDBBeanClass failed")
      collection.map(a=> a.toString).foreach(print(_))
      false
    }
  }

  def testMaprDBBeanClassWithCollect(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB[BeanTest.User](tableName)
    val collection = maprd.map(a => a.toString).collect
    if (collection.toSet.equals(Set("User(rsmith,Robert,Smith,1982-02-03,List(electronics, music, sports))",
      "User(mdupont,Maxime,Dupont,1982-02-03,List(sports, movies, electronics))",
      "User(jdoe,John,Doe,1970-06-23,null)",
      "User(dsimon,David,Simon,1980-10-13,null)",
      "User(alehmann,Andrew,Lehmann,1980-10-13,List(html, css, js))")))
      {
        println("testMaprDBBeanClassWithCollect succeded")
        true
      }
    else {
      println("testMaprDBBeanClassWithCollect failed")
      collection.map(a=> a.toString).foreach(print(_))
      false
    }
  }

  def testGroupByOnODate(sparkSession: SparkContext, tableName : String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val aggregatedondate = maprd.map(a => (a.dob[ODate],a)).groupByKey().map(a => (a._1, a._2.size))
    val collection = aggregatedondate.collect

    if (collection.toSet.sameElements(Set((ODate.parse("1982-02-03"),2),(ODate.parse("1980-10-13"), 2),(ODate.parse("1970-06-23"),1))))
      {
        println("testGroupByOnODate succeded")
        true
      }
    else {
      println("testGroupByOnODate failed")
      false
    }
  }

  def testingLoadingOfOJAIDocuments(sparkSession: SparkContext, tableName: String) = {

    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd.collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")))
      {
        println("testingLoadingOfOJAIDocuments succeded")
        true
      }
    else {
      println("testingLoadingOfOJAIDocuments failed")
      false
    }
  }

  def testingCoGroupWithOJAIDOCUMENT(sparkSession: SparkContext, tableName: String) = {
    val maprd1 = sparkSession.loadFromMapRDB(tableName)
    val maprd2 = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd1.map(a => (a.`address.zip`[Integer], a))
      .cogroup(maprd2.map(a => (a.`address.zip`[Integer], a)))
      .map(a => (a._1, a._2._1.size, a._2._2.size))
      .collect
    if (collection.sameElements(Set((null, 3, 3), (94105, 1, 1), (95109, 1, 1))))
      {
        println("testingCoGroupWithOJAIDocument succeded")
        true
      }
    else {
      println("testingCoGroupWithOJAIDocument failed")
      false
    }
  }

  def testingArrayValueWithOJAIDocument(sparkSession: SparkContext, tableName: String) = {
    val maprd1 = sparkSession.loadFromMapRDB(tableName)
    val maprd2 = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd1.map(a => (a.`address.zip`[Integer], a))
      .cogroup(maprd2.map(a => (a.`address.zip`[Integer], a.interests)))
      .map(a => (a._1, a._2._1.size, a._2._2.size))
      .collect
    if (collection.toSet.sameElements(Set((null, 3, 3), (94105, 1, 1), (95109, 1, 1))))
      {
        println("testingArrayValueWithOJAIDocument succeded")
        true
      }
    else {
      println("testingArrayValueWithOJAIDocument failed")
      false
    }
  }

  def testingCoGroupArrayKey(sparkSession: SparkContext, tableName: String) = {
    val maprd1 = sparkSession.loadFromMapRDB(tableName)
    val maprd2 = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd1.map(a => (a.interests, a))
      .cogroup(maprd2.map(a => (a.interests, a.interests)))
      .map(a => (a._1, a._2._1.size, a._2._2.size))
      .collect
    if (collection.toSet.equals(Set((null, 2, 2), (Seq("html","css","js"), 1, 1), (Seq("sports", "movies", "electronics"), 1, 1), (Seq("electronics", "music",
      "sports"),1,1))))
      {
        println("testingCoGroupArrayKey succeded")
        true
      }
    else {
      println("testingCoGroupArrayKey failed")
      false
    }
  }

  def testingCoGroupOnMapValue(sparkSession: SparkContext, tableName:String) = {
    val maprd1 = sparkSession.loadFromMapRDB(tableName)
    val maprd2 = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd1.map(a => (a.`address.zip`[Integer], a))
      .cogroup(maprd2.map(a => (a.`address.zip`[Integer], a.address)))
      .map(a => (a._1, a._2._1.size, a._2._2.size))
      .collect
    if (collection.toSet.sameElements(Set((null, 3, 3), (94105, 1, 1), (95109, 1, 1))))
      {
        println("testingCoGroupOnMapValue succeded")
        true
      }
    else {
      println("testingCoGroupOnMapValue failed")
      false
    }
  }

  def testingCoGroupOnMapKey(sparkSession: SparkContext, tableName: String) = {
    val maprd1 = sparkSession.loadFromMapRDB(tableName)
    val maprd2 = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd1.map(a => (a.address, a)).cogroup(maprd2.map(a => (a.address, a.address))).map(a => (a._1, a._2._1.size, a._2._2.size))
      .collect
    if (collection.map(a => a.toString()).toSet.equals(Set("(null,3,3)","(Map(city -> San " +
      "Francisco, line -> 100 Main Street, zip -> 94105.0),1,1)","(Map(city -> San Jose, line -> 1223 Broadway, zip -> 95109.0),1,1)")))
      {
        println("testingCoGroupOnMapKey succeded")
        true
      }
    else {
      println("testingCoGroupOnMapKey failed")
      false
    }
  }

  def testingCoGroupWithMapKeyWithFilter(sparkSession: SparkContext, tableName: String) = {
    val maprd1 = sparkSession.loadFromMapRDB(tableName)
    val maprd2 = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd1.map(a => (a.address, a))
      .cogroup(maprd2.map(a => (a.address, a.address)))
      .map(a => (a._1, a._2._1.size, a._2._2.size))
      .filter(a => a._1!= null)
      .collect
    if (collection.map(a => a.toString()).toSet.equals(Set("(Map(city -> San Francisco, line -> 100 Main Street, zip -> 94105.0),1,1)","(Map(city -> San Jose, line -> 1223 " +
      "Broadway, zip -> 95109.0),1,1)")))
      {
        println("testingCoGroupWithMapKeyWithFilter succeded")
        true
      }
    else {
      println("testingCoGroupWithMapKeyWithFilter failed")
      false
    }
  }

  def testingAssignmentOfZipCodeToOJAIDocument(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd.map(a => { a.`address.zip` =  95035 ; a }).collect
    if (collection.map(a => a.`address.zip`).toSet.sameElements(Set(95035,95035,95035,95035,95035)))
      {
        println("testingAssignmentOfZipCodeToOJAIDocument succeded")
        true
      }
    else {
      println("testingAssignmentOfZipCodeToOJAIDocument failed")
      false
    }
  }

  def testingAccessOfFieldsOfOJAIDocumentWithParametricTypes(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd.map(a => a.`address.zip`).collect

    if(collection.toSet.equals(Set(94105,95109, null, null, null)))
      {
        println("testingAccessOfFieldsOfOJAIDocumentWithParametricTypes succeded")
        true
      }
    else {
      println("testingAccessOfFieldsOfOJAIDocumentWithParametricTypes failed")
      false
    }
  }

  def testingAccessOfFieldsOfOJAIDocumentWithParametricTypes2(sparkSession: SparkContext, tableName: String) = {

    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd.map(a => a.`address.zip`[Integer]).collect
    if (collection.toSet.equals(Set(94105,95109, null, null, null)))
      {
        println("testingAccessOfFieldsOfOJAIDocumentWithParametricTypes2 succeded")
        true
      }
    else {
      println("testingAccessOfFieldsOfOJAIDocumentWithParametricTypes2 failed")
      false
    }
  }

  def testingAccessOfProjectedFields(sparkSession: SparkContext, tableName: String) = {

    val maprd = sparkSession.loadFromMapRDB(tableName).select("address", "_id", "first_name")
    val collection = maprd.collect()
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"first_name\":\"Robert\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"first_name\":\"Maxime\"}",
        "{\"_id\":\"jdoe\",\"first_name\":\"John\"}",
        "{\"_id\":\"dsimon\",\"first_name\":\"David\"}",
        "{\"_id\":\"alehmann\",\"first_name\":\"Andrew\"}")))
      {
        println("testingAccessOfProjectedFields succeded")
        true
      }
    else {
      println("testingAccessOfProjectedFields failed")
      false
    }
  }

  def testingAccessOfProjectedFieldPaths(sparkSession: SparkContext, tableName: String) = {

    val maprd = sparkSession.loadFromMapRDB(tableName).select(FieldPath.parseFrom("address"), FieldPath.parseFrom("_id"), FieldPath.parseFrom("first_name"))
    val collection = maprd.collect()
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"first_name\":\"Robert\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"first_name\":\"Maxime\"}",
        "{\"_id\":\"jdoe\",\"first_name\":\"John\"}",
        "{\"_id\":\"dsimon\",\"first_name\":\"David\"}",
        "{\"_id\":\"alehmann\",\"first_name\":\"Andrew\"}")))
    {
      println("testingAccessOfProjectedFieldPaths succeded")
      true
    }
    else {
      println("testingAccessOfProjectedFieldPaths failed")
      false
    }
  }

  def testingOfSavingTheProcessedOJAIDocuments(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists("/tmp/temp_user_profiles"))
      MapRDB.deleteTable("/tmp/temp_user_profiles")
    val maprd = sparkSession.loadFromMapRDB(tableName).select("address", "id", "first_name")
    maprd.saveToMapRDB("/tmp/temp_user_profiles", createTable = true)
    val savedDocumentsRDD = sparkSession.loadFromMapRDB("/tmp/temp_user_profiles")
    val collection = savedDocumentsRDD.collect()
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"first_name\":\"Robert\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"first_name\":\"Maxime\"}",
        "{\"_id\":\"jdoe\",\"first_name\":\"John\"}",
        "{\"_id\":\"dsimon\",\"first_name\":\"David\"}",
        "{\"_id\":\"alehmann\",\"first_name\":\"Andrew\"}")))
      {
        println("testingOfSavingTheProcessedOJAIDocuments succeded")
        true
      }
    else {
      println("testingOfSavingTheProcessedOJAIDocuments failed")
      false
    }
  }

  def testingMapAsaValue(sparkSession: SparkContext, tableName : String) = {

    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd.map(a => a.address).collect
    if (collection.map(a => { Option(a).map(a => a.asInstanceOf[Map[String, Any]]).getOrElse(null)}).toSet.equals(Set(Map("city" -> "San Francisco", "line" -> "100 Main Street",
      "zip" -> 94105), Map("city" -> "San Jose", "line" -> "1223 Broadway", "zip" -> 95109), null, null, null))) {
      println("testingMapAsaValue succeded")
      true
    } else {
      println("testingMapAsaValue failed")
      false
    }
  }

  def testingMapAsaKey(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd.map(a => (a.address,a.first_name)).collect
    val newcollection = collection.map(a => Option(a._1).map(a => a.asInstanceOf[Map[String, AnyRef]]).getOrElse(null)).toSet

    if (newcollection.equals(Set(null, Map("city" -> "San Jose",
      "line" -> "1223 Broadway", "zip" -> 95109),
      Map("city" -> "San Francisco", "line" -> "100 Main Street", "zip" -> 94105), null, null))) {
      println("testingMapAsaKey succeded")
      true
    } else {
      println("testingMapAsaKey failed")
      false
    }
  }

  def testingArrayAsaValue(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection: Seq[Object] = maprd.map(a => a.interests).collect
    if (collection.map(a => { if (a !=null)  a.asInstanceOf[Seq[AnyRef]] else null}).toSet.equals(Set(Seq("electronics","music","sports"),Seq("sports",
      "movies",
      "electronics"),Seq("html", "css","js"), null, null)))
      {
        println("testingArrayAsaValue succeded")
        true
      }
    else {
      println("testingArrayAsaValue failed")
      false
    }
  }

  def testingArrayAsaKey(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val collection = maprd.map(a => (a.interests,a)).collect

    if (collection.map(a => { if (a._1 != null) a._1.asInstanceOf[Seq[AnyRef]] else null}).toSet.equals(Set(Seq("electronics","music","sports"),Seq("sports","movies",
      "electronics"),Seq("html", "css","js"), null, null))) {
      println("testingArrayAsaKey succeded")
      true
    } else {
      println("testingArrayAsaKey failed")
      false
    }
  }

  def testingOJAIDocumentParsingFunctionality(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists("/tmp/testingOJAIDocumentParsingFunctionality"))
      MapRDB.deleteTable("/tmp/testingOJAIDocumentParsingFunctionality")
    val documents = sparkSession.parallelize((1 to 10).map( i => s"{\42_id\42 : \42$i\42, \42test\42: $i}"))

    val maprd = documents.map(a => MapRDBSpark.newDocument(a))
    maprd.saveToMapRDB("/tmp/testingOJAIDocumentParsingFunctionality", createTable = true)

    val saveddocs = sparkSession.loadFromMapRDB("/tmp/testingOJAIDocumentParsingFunctionality")
    val collection = saveddocs.map(a => a.test).collect()
    if (collection.toSet.equals(Set(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0)))
      {
        println("testingOJAIDocumentParsingFunctionality succeded")
        true
      }
    else {
      println("testingOJAIDocumentParsingFunctionality failed")
      false
    }
  }

  def testingMultipleDataTypesInOJAIDocument(sparkSession: SparkContext, tableName: String) = {
    val documents = sparkSession.parallelize((1 to 10).map( i => {
      if (i < 5) s"{\42test\42: $i}"
      else s"{\42test\42: \42$i\42}"
    }))

    val maprd = documents.map(a => MapRDBSpark.newDocument(a))
    val collection = maprd.map(a => a.test).collect

    if (collection.toSet.equals(Set("8", "9", 1.0, "5", "10", 2.0, "6", 3.0, "7", 4.0)))
      {
        println("testingMultipleDataTypesInOJAIDocument succeded")
        true
      }
    else {
      println("testingMultipleDataTypesInOJAIDocument failed")
      false
    }
  }

  def testingSingleDataTypeDoubleInOJAIAndTypeCasting(sparkSession: SparkContext, tableName : String) = {
    val documents = sparkSession.parallelize((1 to 10).map( i => {
      s"{\42test\42: $i.$i}"
    }))

    val maprd = documents.map(a => MapRDBSpark.newDocument(a))
    val collection = maprd.map(a => a.test[Integer]).collect
    if (collection.toSet.equals(Set(8, 9, 1, 5, 10, 2, 6, 3, 7, 4)))
      {
        println("testingSingleDataTypeDoubleInOJAIAndTypeCasting succeded")
        true
      }
    else {
      println("testingSingleDataTypeDoubleInOJAIAndTypeCasting failed")
      false
    }
  }

  def testingTupleOutputOfAnRDD(sparkSession: SparkContext, tableName : String) = {
    val documents = sparkSession.loadFromMapRDB(tableName)
    val collection = documents.map(a => (a.`address.city`,a.`address.zip`,a.first_name, a.last_name)).collect()
    if(collection.toSet.equals(Set(("San Francisco",94105,"Robert","Smith"),
      ("San Jose",95109,"Maxime","Dupont"), (null,null,"John","Doe"), (null,null,"David","Simon"), (null, null,"Andrew","Lehmann"))))
      {
        println("testingTupleOutputOfAnRDD succeded")
        true
      }
    else {
      println("testingTupleOutputOfAnRDD failed")
      false
    }
  }

  def testingAddingCountryInAddressField(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val documents = maprd.map(a => { a.`address.country` = "USA"; a.`address.street` = 10; a}).map(a => a.`address.street`[Integer]).collect
    if (documents.toSet.sameElements(Set(10,10, 10, 10, 10)))
      {
        println("testingAddingCountryInAddressField succeded")
        true
      }
    else {
      println("testingAddingCountryInAddressField failed")
      false
    }
  }

  def testCaseWithFlatMap(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val documents = maprd.flatMap(a => Array(a.`address`)).collect
    if (documents.toSet.equals(Set(Map("city" -> "San Francisco", "line" -> "100 Main Street",
      "zip" -> 94105), Map("city" -> "San Jose", "line" -> "1223 Broadway", "zip" -> 95109), null, null, null)))
    {
      println("testingAddingCountryInAddressField succeded")
      true
    }
    else {
      println("testingAddingCountryInAddressField failed")
      documents.foreach(println(_))
      false
    }
  }

  def testingBinaryDataTypeInOJAIDocument(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val buff: ByteBuffer = ByteBuffer.wrap("Non-Immigrant".getBytes( Charset.forName("UTF-8" )))
    val str: String = new String( buff.array(), Charset.forName("UTF-8") )
    val dBBinaryValue = MapRDBSpark.serializableBinaryValue(buff)
    val map = Map("hello"-> "h", "bolo" -> "b")
    val documents = maprd.map(a => { a.`address.country` = "USA"; a.user_profile = dBBinaryValue; a })
      .map(a => a.getBinarySerializable("user_profile")).collect
    val chars: Seq[String] = documents.map(a=> new String(a.array(), Charset.forName("UTF-8")))
    if (chars.sameElements(Seq("Non-Immigrant","Non-Immigrant", "Non-Immigrant", "Non-Immigrant",
      "Non-Immigrant")))
      {
        println("testingBinaryDataTypeInOJAIDocument succeded")
        true
      }
    else {
      println("testingBinaryDataTypeInOJAIDocument failed")
      false
    }
  }

  def testingMapTypeInOJAIDocument(sparkSession: SparkContext, tableName : String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val buff: ByteBuffer = ByteBuffer.wrap("Non-Immigrant".getBytes( Charset.forName("UTF-8" )))
    val str: String = new String( buff.array(), Charset.forName("UTF-8") )
    val dBBinaryValue = MapRDBSpark.serializableBinaryValue(buff)
    val map = Map("hello"-> "h",
      "bolo" -> "b")

    val documents = maprd.map(a => {a.user_profile = Seq("A","b"); a })
      .map(a => a.`user_profile`).take(5)
    if (documents.map(a => a.asInstanceOf[Seq[AnyRef]]).sameElements(Array(Seq("A", "b"), Seq("A", "b"), Seq("A", "b"), Seq("A", "b"), Seq("A", "b"))))
      {
        println("testingMapTypeInOJAIDocument succeded")
        true
      }
    else {
      println("testingMapTypeInOJAIDocument failed")
      false
    }
  }

  def testingDateTypeInOJAIDocument(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val buff: ByteBuffer = ByteBuffer.wrap("Non-Immigrant".getBytes( Charset.forName("UTF-8" )))
    val str: String = new String( buff.array(), Charset.forName("UTF-8") )
    val dBBinaryValue = MapRDBSpark.serializableBinaryValue(buff)
    val documents = maprd.map(a => { a.`address.country` = "USA"; a.user_profile = ODate.parse("2011-10-10"); a })
      .map(a => a.user_profile).collect
    if (documents.toSet.sameElements(Set(ODate.parse("2011-10-10"), ODate.parse("2011-10-10"), ODate.parse("2011-10-10"), ODate.parse("2011-10-10"), ODate.parse("2011-10-10"))))
      {
        println("testingDateTypeInOJAIDocument succeded")
        true
      }
    else {
      println("testingDateTypeInOJAIDocument failed")
      false
    }
  }

  def testingSaveWithAnyObject(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists("/tmp/testingSaveWithAnyObject"))
      MapRDB.deleteTable("/tmp/testingSaveWithAnyObject")
    val maprd = sparkSession.loadFromMapRDB[BeanTest.User](tableName)
    maprd.saveToMapRDB("/tmp/testingSaveWithAnyObject", bulkInsert = true, createTable = true)
    val savedocuments = sparkSession.loadFromMapRDB("/tmp/testingSaveWithAnyObject").collect
    if (savedocuments.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"dob\":\"1982-02-03\",\"first_name\":\"Robert\",\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"interests\":null,\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"interests\":null,\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}") ))
      {
        println("testingSaveWithAnyObject succeded")
        true
      }
    else {
      println("testingSaveWithAnyObject failed")
      false
    }
  }

  def testingFilterFunctionOnMapObject(sparkSession: SparkContext, tableName: String) =  {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val useraddress = maprd.map(a => a.address[Map[String,AnyRef]]).filter(a => a!= null  && a.get("city").get.equals("San Jose")).collect
    if (useraddress.map(a => a.toSet).toSet.sameElements(Set(Set("city" -> "San Jose", "line" -> "1223 Broadway", "zip" -> 95109))))
      {
        println("testingFilterFunctionOnMapObject succeded")
        true
      }
    else {
      println("testingFilterFunctionOnMapObject failed")
      false
    }
  }

  def testingFilterFunctionOnArrayObject(sparkSession: SparkContext, tableName : String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val useraddress = maprd.map(a => a.interests[Seq[AnyRef]]).filter(a => a!= null && a(0) == "sports").collect
    if (useraddress.map(a => a).toSet.sameElements(Set(Seq("sports", "movies", "electronics"))))
      {
        println("testingFilterFunctionOnArrayObject succeded")
        true
      }
    else {
      println("testingFilterFunctionOnArrayObject failed")
      false
    }
  }

  def testingFilterFunctionOnArrayObjectFunctionalway(sparkSession: SparkContext, tableName : String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName)
    val useraddress = maprd.map(a => a.interests[Seq[AnyRef]]).filter(a => Option(a).map(b => b(0) == "sports").getOrElse(false)).collect
    if (useraddress.map(a => a).toSet.sameElements(Set(Seq("sports", "movies", "electronics"))))
    {
      println("testingFilterFunctionOnArrayObjectFunctionalWay succeded")
      true
    }
    else {
      println("testingFilterFunctionOnArrayObjectFunctionalWay failed")
      useraddress.foreach(println(_))
      false
    }
  }

  def testingWhereClauseOnloadFromMapRDB(sparkSession: SparkContext, tableName: String) = {
    val maprd = sparkSession.loadFromMapRDB(tableName).where(field("address.city") === "San Jose").collect
    if (maprd.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
        "\"electronics\"],\"last_name\":\"Dupont\"}")))
      {
        println("testingWhereClauseOnloadFromMapRDB succeded")
        true
      }
    else {
      println("testingWhereClauseOnloadFromMapRDB failed")
      false
    }
  }

  def testingPartitionOnloadFromMapRDB(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists(saveToTable))
      MapRDB.deleteTable(saveToTable)
    val maprd = sparkSession.loadFromMapRDB(tableName).keyBy(a => a.first_name[String])
                        .repartitionAndSortWithinPartitions(MapRDBSpark.newPartitioner[String](tableName)).values.saveToMapRDB(saveToTable, createTable = true)
    val collection = sparkSession.loadFromMapRDB(saveToTable).collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")))
      {
        println("testingPartitionOnloadFromMapRDB succeded")
        true
      }
    else {
      println("testingPartitionOnloadFromMapRDB failed")
      false
    }
  }

  def testingAssignmentOfDocument(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists(saveToTable+"assigningdoc"))
      MapRDB.deleteTable(saveToTable+"assigningdoc")
    val maprd = sparkSession.loadFromMapRDB(tableName).keyBy(a => a.first_name[String]).mapValues( a => a.set("recursivedoc", MapRDBSpark.newDocument(a.asJsonString)))
      .repartitionAndSortWithinPartitions(MapRDBSpark.newPartitioner[String](tableName)).values.saveToMapRDB(saveToTable+"assigningdoc", createTable= true)
    val collection = sparkSession.loadFromMapRDB(saveToTable+"assigningdoc").collect
    if (collection.map(a => a.delete("recursivedoc")).map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")))
    {
      println("testingAssignmentOfDocument succeded")
      true
    }
    else {
      println("testingAssignmentOfDocument failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingJoinWithRDD(sparkSession: SparkContext, tableName: String) = {
    val valuesRDD = sparkSession.parallelize(Seq("jdoe","Hon","rsmith"))
    val documents = valuesRDD.joinWithMapRDB(tableName).collect
    if (documents.map(doc => doc._id).toSet.sameElements(Set("jdoe","rsmith")))
    {
      println("testingJoinWithRDD succeded")
      true
    }
    else {
      println("testingJoinWithRDD failed")
      documents.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingBulkJoinWithRDD(sparkSession: SparkContext, tableName: String) = {
    val valuesRDD = sparkSession.parallelize(Seq("jdoe","Hon","rsmith"))
    val documents = valuesRDD.bulkJoinWithMapRDB(tableName).collect
    if (documents.map(doc => doc._id).toSet.sameElements(Set("jdoe","rsmith")))
    {
      println("testingBulkJoinWithRDD succeded")
      true
    }
    else {
      println("testingBulkJoinWithRDD failed")
      documents.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingJoinWithRDDBean(sparkSession: SparkContext, tableName: String) = {
    val valuesRDD = sparkSession.parallelize(Seq("jdoe","Hon","rsmith"))
    val documents = valuesRDD.joinWithMapRDB[User](tableName).collect
    if (documents.map(doc => doc.id).toSet.sameElements(Set("jdoe","rsmith")))
    {
      println("testingJoinWithRDDBean succeded")
      true
    }
    else {
      println("testingJoinWithRDDBean failed")
      documents.map(a => a.toString).foreach(println(_))
      false
    }
  }

  def testingJoinWithOjaiRDDBean(sparkSession: SparkContext, tableName: String) = {
    val valuesRDD = sparkSession.loadFromMapRDB(tableName).map(_.getString("_id"))
    val documents = valuesRDD.joinWithMapRDB[User](tableName).collect
    if (documents.map(doc => doc.id).toSet.sameElements(Set("jdoe","rsmith","alehmann", "mdupont", "dsimon")))
    {
      println("testingJoinWithOjaiRDDBean succeded")
      true
    }
    else {
      println("testingJoinWithOjaiRDDBean failed")
      documents.map(a => a.toString).foreach(println(_))
      false
    }
  }

  def testingUpdateMapRDBTable(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists(tableName+"updateMapRDB"))
      MapRDB.deleteTable(tableName+"updateMapRDB")
    sparkSession.loadFromMapRDB(tableName).saveToMapRDB(tableName+"updateMapRDB", createTable = true)
    val valuesRDD = sparkSession.loadFromMapRDB(tableName+"updateMapRDB")
    valuesRDD.updateToMapRDB(tableName+"updateMapRDB", (doc: OJAIDocument) => {
      val mutation = MapRDB.newMutation()
      mutation.setOrReplace("key", doc.getIdString())
      mutation
    }, (doc: OJAIDocument) => doc.getId)
    val collection = sparkSession.loadFromMapRDB(tableName+"updateMapRDB").collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"key\":\"rsmith\",\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"key\":\"mdupont\",\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"key\":\"jdoe\",\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"key\":\"dsimon\",\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"key\":\"alehmann\",\"last_name\":\"Lehmann\"}")))
    {
      println("testingUpdateMapRDBTable succeded")
      true
    }
    else {
      println("testingUpdateMapRDBTable failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingCheckAndUpdateMapRDBTable(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists(tableName+"checkAndUpdateMapRDB"))
      MapRDB.deleteTable(tableName+"checkAndUpdateMapRDB")
    sparkSession.loadFromMapRDB(tableName).saveToMapRDB(tableName+"checkAndUpdateMapRDB", createTable = true)
    val valuesRDD = sparkSession.loadFromMapRDB(tableName+"checkAndUpdateMapRDB")
    valuesRDD.updateToMapRDB(tableName+"checkAndUpdateMapRDB", (doc: OJAIDocument) => {
      val mutation = MapRDB.newMutation()
      mutation.setOrReplace("key", doc.getIdString())
      mutation
    }, (doc: OJAIDocument) => doc.getId, field("_id") === "rsmith")
    val collection = sparkSession.loadFromMapRDB(tableName+"checkAndUpdateMapRDB").collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"key\":\"rsmith\",\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")))
    {
      println("testingCheckAndUpdateMapRDBTable succeded")
      true
    }
    else {
      println("testingCheckAndUpdateMapRDBTable failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingUpdateMapRDBTablePairedRDD(sparkSession: SparkContext, tableName: String) = {
    if (MapRDB.tableExists(tableName+"updateMapRDB"))
      MapRDB.deleteTable(tableName+"updateMapRDB")
    sparkSession.loadFromMapRDB(tableName).saveToMapRDB(tableName+"updateMapRDB", createTable = true)
    val valuesRDD = sparkSession.loadFromMapRDB(tableName+"updateMapRDB").keyBy(_.getIdString)
    valuesRDD.updateToMapRDB(tableName+"updateMapRDB", (doc: OJAIDocument) => {
      val mutation = MapRDB.newMutation()
      mutation.setOrReplace("key", doc.getIdString())
      mutation
    })
    val collection = sparkSession.loadFromMapRDB(tableName+"updateMapRDB").collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"key\":\"rsmith\",\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"key\":\"mdupont\",\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"key\":\"jdoe\",\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"key\":\"dsimon\",\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"key\":\"alehmann\",\"last_name\":\"Lehmann\"}")))
    {
      println("testingUpdateMapRDBTablePairedRDD succeded")
      true
    }
    else {
      println("testingUpdateMapRDBTablePairedRDD failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }
}

object BeanTest {
  case class person(name: String, age: Integer)

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class User (@JsonProperty("_id") id: String, @JsonProperty("first_name") firstName:String,
                   @JsonProperty("last_name") lastName: String, @JsonProperty("dob") dob: ODate,
                   @JsonProperty("interests") interests: Seq[String])

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class User1 (@JsonProperty("_id") id: String, @JsonProperty("first_name") firstName:String,
                   @JsonProperty("last_name") lastName: Option[Int], @JsonProperty("dob") dob: ODate,
                   @JsonProperty("interests") interests: Seq[String])

}


object MapRDBSparkTestsWithKryo {
  val tableName = "/tmp/user_profiles_temp"
  val saveToTable = "/tmp/user_profiles_temp_save"
  lazy val conf = new SparkConf()
    .setAppName("simpletest")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(conf)
    MapRDBSparkTests.tableInitialization(sc, tableName)
    MapRDBSparkTests.runTests(sc)
  }
}
