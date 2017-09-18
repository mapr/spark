/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.testCases

import java.util.Collections

import com.mapr.db.MapRDB
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.mapr.db.spark.sql._
import com.mapr.db.testCases.BeanTest1.User1
import org.apache.spark.sql.types._
import com.mapr.db.MapRDB
import com.mapr.db.rowcol.{DBValueBuilder, DBValueBuilderImpl}
import com.mapr.db.spark._
import org.ojai.types.ODate


object SparkSqlLoadAndSaveTests {
  lazy val conf = new SparkConf()
    .setAppName("SparkSqlLoadAndSaveTests")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
       .setMaster("local[*]")

  lazy val spark = SparkSession.builder().appName("SparkSqlLoadAndSaveTests").config(conf).getOrCreate()

  val tableName = "/tmp/SparkSqlOjaiConnectorLoadAndSaveTesting"
  val saveTableName = "/tmp/SparkSqlOjaiConnectorLoadAndSaveTesting_save"

  def main(args: Array[String]): Unit = {
    MapRDBSparkTests.tableInitialization(spark.sparkContext, tableName)
    SparkSqlLoadAndSaveTests.runTests(spark)
  }

  def testingLoadBeanClass(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val users : DataFrame = spark.loadFromMapRDB[User1](tableName)
    val andrewProfile : Array[String] = users.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingLoadBeanClass succeeded")
      return true
    }
    else {
      println("testingLoadBeanClass failed")
      return false
    }
  }

  def testingLoadExplicitSchema(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val addressSchema = StructType(StructField("Pin", IntegerType) ::
      StructField("city", StringType) ::
      StructField("street", StringType) :: Nil)

    val schema = StructType(StructField("_id", StringType) ::
      StructField("first_name", StringType) ::
      StructField("last_name", StringType) ::
      StructField("address", addressSchema) ::
      StructField("interests", ArrayType(StringType)) :: Nil)

    val users : DataFrame = spark.loadFromMapRDB(tableName, schema)
    val andrewProfile : Array[String] = users.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingLoadExplicitSchema succeeded")
      return true
    }
    else {
      println("testingLoadExplicitSchema failed")
      return false
    }
  }

  def testingLoadInferSchema(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val users : DataFrame = spark.loadFromMapRDB(tableName)
    val andrewProfile : Array[String] = users.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingLoadBeanClass succeeded")
      return true
    }
    else {
      println("testingLoadBeanClass failed")
      return false
    }
  }

  def testingLoadFromDFReader(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val users : DataFrame = spark.read.maprdb(tableName)
    val andrewProfile : Array[String] = users.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingLoadFromDFReader succeeded")
      return true
    }
    else {
      println("testingLoadFromDFReader failed")
      return false
    }
  }

  def testingLoadFromDFReaderLoad(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val users : DataFrame = spark.read.format("com.mapr.db.spark.sql").option("tableName", tableName).option("sampleSize", 100).load()
    val andrewProfile : Array[String] = users.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingLoadFromDFReaderLoad succeeded")
      return true
    }
    else {
      println("testingLoadFromDFReaderLoad failed")
      return false
    }
  }

  def testingLoadFromDFWriterWithOperationOption(spark: SparkSession, tableName: String): Boolean = {
    if (MapRDB.tableExists(tableName+"writer_save"))
      MapRDB.deleteTable(tableName+"writer_save")
    import spark.implicits._
    val users : DataFrame = spark.read.option("sampleSize", 100).maprdb(tableName)
    users.write.option("Operation", "Insert").saveToMapRDB(tableName+"writer_save")
    val collection = spark.sparkContext.loadFromMapRDB(tableName+"writer_save").collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"address\":null,\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"interests\":null,\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"address\":null,\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"interests\":null,\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"address\":null,\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")))
    {
      println("testingLoadFromDFWriterWithOperationOption succeded")
      true
    }
    else {
      println("testingLoadFromDFWriterWithOperationOption failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingUpdateToMapRDB(spark: SparkSession, tableName: String): Boolean = {
    if (MapRDB.tableExists(tableName+"writer_save"))
      MapRDB.deleteTable(tableName+"writer_save")
    import spark.implicits._
    val users : DataFrame = spark.read.option("sampleSize", 100).maprdb(tableName)
    users.write.saveToMapRDB(tableName+"writer_save")
    users.updateToMapRDB(tableName+"writer_save", (row: Row) => {
      val mutation = MapRDB.newMutation()
      mutation.setOrReplace("key", row.getString(row.fieldIndex("_id")))
      mutation
    }, (row: Row) => DBValueBuilderImpl.KeyValueBuilder.initFrom(row.getString(row.fieldIndex("_id"))))
    val collection = spark.sparkContext.loadFromMapRDB(tableName+"writer_save").collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"key\":\"rsmith\",\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"key\":\"mdupont\",\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"address\":null,\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"interests\":null,\"key\":\"jdoe\",\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"address\":null,\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"interests\":null,\"key\":\"dsimon\",\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"address\":null,\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"key\":\"alehmann\",\"last_name\":\"Lehmann\"}")))
    {
      println("testingUpdateToMapRDB succeded")
      true
    }
    else {
      println("testingUpdateToMapRDB failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingCheckAndUpdateToMapRDB(spark: SparkSession, tableName: String): Boolean = {
    if (MapRDB.tableExists(tableName+"writer_save"))
      MapRDB.deleteTable(tableName+"writer_save")
    import spark.implicits._
    val users : DataFrame = spark.read.option("sampleSize", 100).maprdb(tableName)
    users.write.saveToMapRDB(tableName+"writer_save")
    users.updateToMapRDB(tableName+"writer_save", (row: Row) => {
      val mutation = MapRDB.newMutation()
      mutation.setOrReplace("key", row.getString(row.fieldIndex("_id")))
      mutation
    }, (row: Row) => DBValueBuilderImpl.KeyValueBuilder.initFrom(row.getString(row.fieldIndex("_id"))), field("_id") === "rsmith")
    val collection = spark.sparkContext.loadFromMapRDB(tableName+"writer_save").collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\"],\"key\":\"rsmith\",\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"address\":null,\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"interests\":null,\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"address\":null,\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"interests\":null,\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"address\":null,\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")))
    {
      println("testingCheckAndUpdateToMapRDB succeded")
      true
    }
    else {
      println("testingCheckAndUpdateToMapRDB failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingUpdateToMapRDBAddToArray(spark: SparkSession, tableName: String): Boolean = {
    if (MapRDB.tableExists(tableName+"writer_modify_array"))
      MapRDB.deleteTable(tableName+"writer_modify_array")
    import spark.implicits._
    val docs = scala.collection.immutable.List("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\"," +
      "\"first_name\":\"Robert\"," +
      "\"interests\":[\"electronics\",\"music\",\"sports\"],\"last_name\":\"Smith\"}",
      "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\",\"electronics\"],\"last_name\":\"Dupont\"}",
      "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"last_name\":\"Doe\"}",
      "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"last_name\":\"Simon\"}",
      "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\"],\"last_name\":\"Lehmann\"}")
    val docsrdd = spark.sparkContext.parallelize(docs)
    val ojairdd = docsrdd.map(doc => MapRDBSpark.newDocument(doc)).map(doc => {
      doc.dob = Option(doc.dob) match {
        case Some(a) => ODate.parse(doc.dob[String])
        case None => null
      }
      doc
    }).map( doc => {
      if (doc.getList("interests") == null)
        doc.set("interests", Seq())
      else doc
    }).saveToMapRDB(tableName+"writer_modify_array", createTable = true)
    val users : DataFrame = spark.read.option("sampleSize", 100).maprdb(tableName)
    users.updateToMapRDB(tableName+"writer_modify_array", (row: Row) => {
      val mutation = MapRDB.newMutation()
      mutation.append("interests", Collections.singletonList("cpp"))
      mutation
    }, (row: Row) => DBValueBuilderImpl.KeyValueBuilder.initFrom(row.getString(row.fieldIndex("_id"))))
    val collection = spark.sparkContext.loadFromMapRDB(tableName+"writer_modify_array").collect
    if (collection.map(a => a.asJsonString()).toSet.sameElements(
      Set("{\"_id\":\"rsmith\",\"address\":{\"city\":\"San Francisco\",\"line\":\"100 Main Street\",\"zip\":94105},\"dob\":\"1982-02-03\",\"first_name\":\"Robert\"," +
        "\"interests\":[\"electronics\",\"music\",\"sports\",\"cpp\"],\"last_name\":\"Smith\"}",
        "{\"_id\":\"mdupont\",\"address\":{\"city\":\"San Jose\",\"line\":\"1223 Broadway\",\"zip\":95109},\"dob\":\"1982-02-03\",\"first_name\":\"Maxime\",\"interests\":[\"sports\",\"movies\"," +
          "\"electronics\",\"cpp\"],\"last_name\":\"Dupont\"}",
        "{\"_id\":\"jdoe\",\"dob\":\"1970-06-23\",\"first_name\":\"John\",\"interests\":[\"cpp\"],\"last_name\":\"Doe\"}",
        "{\"_id\":\"dsimon\",\"dob\":\"1980-10-13\",\"first_name\":\"David\",\"interests\":[\"cpp\"],\"last_name\":\"Simon\"}",
        "{\"_id\":\"alehmann\",\"dob\":\"1980-10-13\",\"first_name\":\"Andrew\",\"interests\":[\"html\",\"css\",\"js\",\"cpp\"],\"last_name\":\"Lehmann\"}")))
    {
      println("testingUpdateToMapRDBAddToArray succeded")
      true
    }
    else {
      println("testingUpdateToMapRDBAddToArray failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def testingLoadFromDFReaderWithOperationOption(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val users : DataFrame = spark.read.option("sampleSize", 100).option("Operation","Insert").maprdb(tableName)
    val andrewProfile : Array[String] = users.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingLoadFromDFReaderWithOperationOption succeeded")
      return true
    }
    else {
      println("testingLoadFromDFReaderWithOperationOption failed")
      return false
    }
  }

  def tesitngLoadFromDFReaderWithSampleOption(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val users : DataFrame = spark.read.format("com.mapr.db.spark.sql").option("tableName", tableName).option("sampleSize", 1000).maprdb(tableName)
    val andrewProfile : Array[String] = users.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("tesitngLoadFromDFReaderWithSampleOption succeeded")
      return true
    }
    else {
      println("tesitngLoadFromDFReaderWithSampleOption failed")
      return false
    }
  }

  def testingLoadFromDFReaderWithFailOnConflict(spark: SparkSession, tableName: String): Boolean = {
    import spark.implicits._
    val users : DataFrame = spark.read.option("sampleSize", 1000).option("FailOnConflict","true").maprdb(tableName)
    val andrewProfile : Array[String] = users.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingLoadFromDFReaderWithFailOnConflict succeeded")
      return true
    }
    else {
      println("testingLoadFromDFReaderWithFailOnConflict failed")
      return false
    }
  }

  def testingSaveFromSparkSession(spark: SparkSession, tableName: String): Boolean = {
    if (MapRDB.tableExists(saveTableName))
      MapRDB.deleteTable(saveTableName)
    import spark.implicits._
    val users : DataFrame = spark.read.option("sampleSize", 1000).option("FailOnConflict","true").maprdb(tableName)
    users.saveToMapRDB(saveTableName, createTable = true)
    val savedUsers : DataFrame = spark.read.format("com.mapr.db.spark.sql").option("tableName", tableName).option("sampleSize", 1000).option("FailOnConflict","true").maprdb(saveTableName)
    val andrewProfile : Array[String] = savedUsers.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingSaveFromSparkSession succeeded")
      return true
    }
    else {
      println("testingSaveFromSparkSession failed")
      return false
    }
  }

  def testingSaveFromDFWSession(spark: SparkSession, tableName: String): Boolean = {
    if (MapRDB.tableExists(saveTableName))
      MapRDB.deleteTable(saveTableName)
    import spark.implicits._
    val users : DataFrame = spark.read.option("sampleSize", 1000).maprdb(tableName)
    users.saveToMapRDB(saveTableName, createTable = true)
    val savedUsers : DataFrame = spark.read.option("sampleSize", 1000).maprdb(saveTableName)
    val andrewProfile : Array[String] = savedUsers.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingSaveFromDFWSession succeeded")
      return true
    }
    else {
      println("testingSaveFromDFWSession failed")
      return false
    }
  }

  def testingSaveWithBulkLoad(spark: SparkSession, tableName: String): Boolean = {
    if (MapRDB.tableExists(saveTableName))
      MapRDB.deleteTable(saveTableName)
    import spark.implicits._
    val users : DataFrame = spark.read.option("sampleSize", 1000).maprdb(tableName)
    users.saveToMapRDB(saveTableName, createTable = true , bulkInsert = true)
    val savedUsers : DataFrame = spark.read.option("sampleSize", 1000).maprdb(saveTableName)
    val andrewProfile : Array[String] = savedUsers.filter($"first_name" === "Andrew").collect.map(r => r.getString(r.fieldIndex("first_name")))
    if (andrewProfile.toSet.equals(Set("Andrew"))) {
      println("testingSaveWithBulkLoad succeeded")
      return true
    }
    else {
      println("testingSaveWithBulkLoad failed")
      return false
    }
  }

  def testingSaveWithComplexDocument(spark: SparkSession): Boolean = {
    if (MapRDB.tableExists(saveTableName+"complex"))
      MapRDB.deleteTable(saveTableName+"complex")

    if (MapRDB.tableExists(saveTableName+"complex_save"))
      MapRDB.deleteTable(saveTableName+"complex_save")

    import com.mapr.db.spark._
    val listOfDocs = List("{\"_id\":\"2DT3201\",\"brand\":\"Careen\",\"category\":\"Pedals\",\"features\":[\"Low-profile design\",\"Floating SH11 cleats included\"],\"name\":\" Allegro SPD-SL 6800\",\"price\":112.99,\"product_ID\":\"2DT3201\",\"specifications\":{\"color\":\"black\",\"weight_per_pair\":\"260g\"},\"type\":\"Components\"}",
    "{\"_id\":\"3ML6758\",\"brand\":\"Careen\",\"category\":\"Jersey\",\"features\":[\"Wicks away moisture.\",\"SPF-30\",\"Reflects light at night.\"],\"name\":\"Trikot 24-LK\",\"price\":76.99,\"product_ID\":\"3ML6758\",\"specifications\":{\"colors\":[\"white\",\"navy\",\"green\"],\"sizes\":[\"S\",\"M\",\"L\",\"XL\",\"XXL\"]},\"type\":\"Clothing\"}",
      "{\"_id\":\"4GGC859\",\"brand\":\"Careen\",\"category\":\"Bicycle\",\"name\":\"Thresher 1000\",\"price\":2949.99,\"product_ID\":\"4GGC859\",\"specifications\":{\"frameset\":{\"fork\":\"Gabel 2\",\"frame\":\"Carbon Enduro\"},\"groupset\":{\"brake\":\"Bremse FullStop\",\"chainset\":\"Kette 230\"},\"size\":\"55cm\",\"wheel_size\":\"700c\",\"wheelset\":{\"tyres\":\"Reifen Pro\",\"wheels\":\"Rad Schnell 10\"}},\"type\":\"Road bicycle\"}")
    val schema = StructType(StructField("_id",StringType,true) ::
      StructField("brand",StringType,true) ::
      StructField("category",StringType,true) ::
      StructField("features",ArrayType(StringType,true),true) ::
      StructField("name",StringType,true) ::
      StructField("price",DoubleType,true) ::
      StructField("product_ID",StringType,true) ::
      StructField("specifications",StructType(StructField("color",StringType,true) ::
        StructField("colors",ArrayType(StringType,true),true) ::
        StructField("frameset",StructType(StructField("fork",StringType,true) ::
          StructField("frame",StringType,true) :: Nil),true) ::
        StructField("groupset",StructType(StructField("brake",StringType,true) ::
          StructField("chainset",StringType,true) :: Nil),true) ::
        StructField("size",StringType,true) ::
        StructField("sizes",ArrayType(StringType,true),true) ::
        StructField("weight_per_pair", StringType, true) ::
        StructField("wheel_size", StringType, true) ::
        StructField("wheelset", StructType(StructField("tyres", StringType, true) ::
          StructField("wheels", StringType, true) :: Nil)) :: Nil)) ::
      StructField("type", StringType, true) :: Nil)

//    val listOfRows = listOfDocs.map(doc => MapRDBSpark.newDocument(doc)).map(doc => MapRDBSpark.docToRow(doc, schema))
//    val docs = listOfRows.map(row => MapRDBSpark.rowToDoc(row)).map(_.asJsonString())
//    return true
    spark.sparkContext.parallelize(listOfDocs).map(MapRDBSpark.newDocument(_)).saveToMapRDB(saveTableName+ "complex", createTable = true)
    val df = spark.read.format("com.mapr.db.spark.sql.DefaultSource").option("tableName", saveTableName+ "complex").load();
//    df.printSchema()
    df.write.format("com.mapr.db.spark.sql.DefaultSource").option("tableName", saveTableName+ "complex_save").save()
    val collection = spark.sparkContext.loadFromMapRDB(saveTableName+"complex_save").collect
    if (collection.map(_.asJsonString()).toSet.equals(Set("{\"_id\":\"2DT3201\",\"brand\":\"Careen\",\"category\":\"Pedals\",\"features\":[\"Low-profile design\",\"Floating SH11 cleats included\"],\"name\":\" Allegro SPD-SL 6800\",\"price\":112.99,\"product_ID\":\"2DT3201\",\"specifications\":{\"color\":\"black\",\"colors\":null,\"frameset\":null,\"groupset\":null,\"size\":null,\"sizes\":null,\"weight_per_pair\":\"260g\",\"wheel_size\":null,\"wheelset\":null},\"type\":\"Components\"}",
    "{\"_id\":\"3ML6758\",\"brand\":\"Careen\",\"category\":\"Jersey\",\"features\":[\"Wicks away moisture.\",\"SPF-30\",\"Reflects light at night.\"],\"name\":\"Trikot 24-LK\",\"price\":76.99,\"product_ID\":\"3ML6758\",\"specifications\":{\"color\":null,\"colors\":[\"white\",\"navy\",\"green\"],\"frameset\":null,\"groupset\":null,\"size\":null,\"sizes\":[\"S\",\"M\",\"L\",\"XL\",\"XXL\"],\"weight_per_pair\":null,\"wheel_size\":null,\"wheelset\":null},\"type\":\"Clothing\"}",
      "{\"_id\":\"4GGC859\",\"brand\":\"Careen\",\"category\":\"Bicycle\",\"features\":null,\"name\":\"Thresher 1000\",\"price\":2949.99,\"product_ID\":\"4GGC859\",\"specifications\":{\"color\":null,\"colors\":null,\"frameset\":{\"fork\":\"Gabel 2\",\"frame\":\"Carbon Enduro\"},\"groupset\":{\"brake\":\"Bremse FullStop\",\"chainset\":\"Kette 230\"},\"size\":\"55cm\",\"sizes\":null,\"weight_per_pair\":null,\"wheel_size\":\"700c\",\"wheelset\":{\"tyres\":\"Reifen Pro\",\"wheels\":\"Rad Schnell 10\"}},\"type\":\"Road bicycle\"}")))     {
      println("testingSaveWithComplexDocument succeded")
      true
    }
    else {
      println("testingSaveWithComplexDocument failed")
      collection.map(a => a.asJsonString).foreach(println(_))
      false
    }
  }

  def runTests(sparkSession: SparkSession): Unit = {
    testingLoadExplicitSchema(spark, tableName)
    testingLoadBeanClass(spark, tableName)
    testingLoadInferSchema(spark, tableName)
    testingLoadFromDFReader(spark, tableName)
    testingLoadFromDFReaderLoad(spark, tableName)
    testingLoadFromDFReaderWithOperationOption(spark, tableName)
    tesitngLoadFromDFReaderWithSampleOption(spark, tableName)
    testingLoadFromDFReaderWithFailOnConflict(spark, tableName)
    testingSaveFromSparkSession(spark, tableName)
    testingSaveFromDFWSession(spark, tableName)
    testingSaveWithBulkLoad(spark, tableName)
    testingSaveWithComplexDocument(spark)
  }
}

object SparkSqlLoadAndSaveTestsWithKryo {

  val tableName = "/tmp/SparkSqlOjaiConnectorLoadAndSaveTesting"
  val saveTableName = "/tmp/SparkSqlOjaiConnectorLoadAndSaveTesting_save"

  lazy val conf = new SparkConf()
    .setAppName("SparkSqlLoadAndSaveTestsWithKryo")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "1g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")

  lazy val spark = SparkSession.builder().appName("SparkSqlLoadAndSaveTestsWithKryo").config(conf).getOrCreate()


  def main(args: Array[String]): Unit = {
    MapRDBSparkTests.tableInitialization(spark.sparkContext,tableName)
    SparkSqlLoadAndSaveTests.runTests(spark)
  }
}


object BeanTest1 {
  case class person(name: String, age: Integer)

  case class User1 (_id: String, first_name:String,
                   last_name: String, dob: java.sql.Date,
                   interests: Seq[String])

}
