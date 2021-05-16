/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db

import java.nio.ByteBuffer

import com.mapr.db.impl.IdCodec
import com.mapr.db.spark.RDD._
import com.mapr.db.spark.condition.quotes
import com.mapr.db.spark.types.DBBinaryValue
import com.mapr.db.spark.writers.{OJAIKey, OJAIValue}
import com.mapr.org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD

package object spark {

  implicit val ojaiStringKeyOrdering = new Ordering[String] {
    override def compare(a: String, b: String) = {
      a.compareTo(b)
    }
  }

  implicit val ojaiDBBinaryKeyOrdering = new Ordering[DBBinaryValue] {
    override def compare(a: DBBinaryValue, b: DBBinaryValue) = {
      val prevBuf = IdCodec.encodeAsBytes(a.getByteBuffer())
      val curBuf = IdCodec.encodeAsBytes(b.getByteBuffer())
      Bytes.compareTo(prevBuf, 0, prevBuf.length, curBuf, 0, curBuf.length)
    }
  }

  implicit val ojaiByteBufferOrdering = new Ordering[ByteBuffer] {
    override def compare(a: ByteBuffer, b: ByteBuffer) = {
      val prevBuf = IdCodec.encodeAsBytes(a)
      val curBuf = IdCodec.encodeAsBytes(b)
      Bytes.compareTo(prevBuf, 0, prevBuf.length, curBuf, 0, curBuf.length)
    }
  }

  /**
    * Spark MapRDB connector specific functions to load json tables as RDD[OJAIDocument]
    * @param sc sparkContext
    * @example val docs = sc.loadMapRDBTable("tablePath")
    */
  implicit def toSparkContextFunctions(
      sc: SparkContext): SparkContextFunctions = SparkContextFunctions(sc)

  /**
    * Spark MapRDB connector specific functions to save either RDD[OJAIDocument]
    *   or RDD of anyobject
    * @param rdd rdd on which this function is called
    * @example docs.saveToMapRDB("tablePath")
    *          It might throw a DecodingException if the RDD
    *          or anyObject is not possible to convert to a document.
    */
  implicit def toDocumentRDDFunctions[D: OJAIValue](rdd: RDD[D]): OJAIDocumentRDDFunctions[D] =
    OJAIDocumentRDDFunctions[D](rdd)

  /**
    * Spark MapRDB connector specific functions to save either RDD[(String, OJAIDocument)]
    *   or RDD[(String, anyobject)]
    * @param rdd rdd on which this function is called
    * @example docs.saveToMapRDB("tablePath")
    *          It might throw a DecodingException if the RDD
    *          or anyObject is not possible to convert to a document.
    */
  implicit def toPairedRDDFunctions[K: OJAIKey, V: OJAIValue](rdd: RDD[(K, V)]
                                                             ): PairedDocumentRDDFunctions[K, V] =
    PairedDocumentRDDFunctions[K, V](rdd)

  /**
    * Spark MapRDB connector specific functions to join external RDD with a MapRDB table.
    * @param rdd rdd on which this function is called
    * @example docs.joinWithMapRDB("tablePath")
    */
  implicit def toFilterRDDFunctions[K: OJAIKey: quotes](rdd: RDD[K]): FilterRDDFunctions[K] =
    FilterRDDFunctions(rdd)
}
