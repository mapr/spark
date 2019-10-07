package org.apache.spark.streaming.kafka.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

package object producer {

  /**
   * Writes data frame of String into MapR-ES.
   *
   * @param dataFrame    data to be written
   * @param sparkSession Spark Session
   */
  implicit class DataFrameOps(dataFrame: DataFrame)(implicit sparkSession: SparkSession) {
    def sendToKafka(topic: String) = {
      dataFrame
        .write
        .format("org.apache.spark.streaming.kafka.producer.sql.KafkaWriter")
        .save(topic)
    }
  }

  /**
   * Writes RDD[String] into MapR-ES.
   *
   * Notice that the JSON written to MapR-ES will be {"data": "<some data here from rdd items>"}
   *
   * @param rdd          data to be written
   * @param sparkSession Spark Session
   */
  implicit class StringRDDOps(rdd: RDD[String])(implicit sparkSession: SparkSession) {
    def sendToKafka(topic: String): Unit =
      toDFAndSendToKafka(topic, rdd.map(Row(_)), new StructType().add("data", StringType))
  }


  /**
   * Writes RDD[A] into MapR-ES.
   *
   * @param rdd          data to be written
   * @param sparkSession Spark Session
   * @tparam A
   */
  implicit class RDDOps[A](rdd: RDD[A])(implicit sparkSession: SparkSession) {

    /**
     * Writes RDD[A] into MapR-ES.
     *
     * @param topic  Kafka topic to write to
     * @param fn     a function to transform each data item in the RDD into a Row
     * @param schema schema of each Row
     */
    def sendToKafka(topic: String, fn: A => Row, schema: StructType): Unit =
      toDFAndSendToKafka(topic, rdd.map(a => fn(a)), schema)
  }

  /**
   * Writes DStream[A] into MapR-ES.
   *
   * @param stream       data to be written
   * @param sparkSession Spark Session
   */
  implicit class StringDStreamOps(stream: DStream[String])(implicit sparkSession: SparkSession) {
    def sendToKafka(topic: String): Unit = stream.foreachRDD(_.sendToKafka(topic))
  }

  /**
   * Writes DStream[A] into MapR-ES.
   *
   * @param stream       data to be written
   * @param sparkSession Spark Session
   */
  implicit class DStreamOps[A](stream: DStream[A])(implicit sparkSession: SparkSession) {

    /**
     * Writes DStream[A] into MapR-ES.
     *
     * @param topic  Kafka topic to write to
     * @param fn     a function to transform each data item in the RDD into a Row
     * @param schema schema of each Row
     */
    def sendToKafka(topic: String, fn: A => Row, schema: StructType): Unit =
      stream
        .map(a => fn(a))
        .foreachRDD(rdd => toDFAndSendToKafka(topic, rdd, schema))
  }

  private def toDFAndSendToKafka(topic: String, rdd: RDD[Row], schema: StructType)(implicit sparkSession: SparkSession): Unit =
    sparkSession
      .createDataFrame(rdd, schema)
      .sendToKafka(topic)

}
