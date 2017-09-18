package com.mapr.db.tests

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.scalatest._
import com.mapr.db.testCases._

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait InitClusterDisableKryoSerialization extends BeforeAndAfterAll {
  this: Suite =>

  private val master = "local[4]"
  private val appName = this.getClass.getSimpleName
  val tableName = "/tmp/user_profiles_read_allTests_sparkOJAIConnector"
  val tableName2 = "/tmp/user_profiles_save_allTests_sparkOJAIConnector"

  private var _sc: SparkContext = _
  private var _ssc: StreamingContext = _

  def sc = _sc
  def ssc = _ssc

  val conf: SparkConf = {
    val cnf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    cnf
  }

  lazy val spark = SparkSession.builder().appName("simpletest").config(conf).getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sc = spark.sparkContext
    _sc.setLogLevel("OFF")
    _ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    com.mapr.db.testCases.MapRDBSparkTests.tableInitialization(sc, MapRDBSparkTests.tableName)
    com.mapr.db.testCases.LoadAndSaveTests.tableInitialization(sc, tableName)
    com.mapr.db.testCases.PredicateTests.tableInitialization(sc, PredicateTests.tableName)
    com.mapr.db.testCases.SparkSqlAccessTests.tableInitialization(SparkSqlAccessTests.tableName)
    com.mapr.db.testCases.MapRDBSparkTests.tableInitialization(sc, SparkSqlPushDownTests.tableName)
    MapRDBSparkTests.tableInitialization(spark.sparkContext, SparkSqlLoadAndSaveTests.tableName)
    MapRDBSparkTests.tableInitialization(spark.sparkContext, SparkStreamingTests.tableName)
  }



  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
      _ssc.stop(false, true)
    }
    super.afterAll()
  }
}



trait InitClusterEnableKryoSerialization extends BeforeAndAfterAll {
  this: Suite =>

  val tableName = "/tmp/user_profiles_read_allTests_sparkOJAIConnector_kryo"
  val tableName2 = "/tmp/user_profiles_save_allTests_sparkOJAIConnector_kryo"
  private val master = "local[4]"
  private val appName = this.getClass.getSimpleName

  private var _sc: SparkContext = _
  private var _ssc: StreamingContext = _

  def sc = _sc
  def ssc = _ssc

  val conf: SparkConf = {
    val cnf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")
    cnf
  }

  lazy val spark = SparkSession.builder().appName("simpletest").config(conf).getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sc = new SparkContext(conf)
    _sc.setLogLevel("OFF")
    _ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    com.mapr.db.testCases.MapRDBSparkTests.tableInitialization(sc, MapRDBSparkTests.tableName)
    com.mapr.db.testCases.LoadAndSaveTests.tableInitialization(sc, tableName)
    com.mapr.db.testCases.PredicateTests.tableInitialization(sc, PredicateTests.tableName)
    com.mapr.db.testCases.SparkSqlAccessTests.tableInitialization(SparkSqlAccessTests.tableName)
    com.mapr.db.testCases.MapRDBSparkTests.tableInitialization(sc, SparkSqlPushDownTests.tableName)
    MapRDBSparkTests.tableInitialization(spark.sparkContext, SparkSqlLoadAndSaveTests.tableName)
    MapRDBSparkTests.tableInitialization(spark.sparkContext, SparkStreamingTests.tableName)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
      _ssc.stop(false, true)
    }
    super.afterAll()
  }
}
