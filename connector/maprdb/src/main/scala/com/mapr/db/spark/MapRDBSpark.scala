/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark

import java.nio.ByteBuffer

import com.mapr.db.spark.RDD.partitioner.{MapRDBPartitioner, MapRDBSplitPartitioner, OJAIKEY}
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.sql.utils.MapRSqlUtils
import com.mapr.db.spark.types.DBBinaryValue
import org.apache.spark.Partitioner
import org.ojai.Document

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
* MapRDBSpark is a static class which contains factory methods to create scala's
*   ojai document and partitioner objects.
* @example Factory functions to help create scala's ojai documents
*          val doc = MapRDBSpark.newDocument(jsonString)
*          val doc = MapRDBSpark.newDocument(document: org.ojai.Document)
*          Here are the ways to access elements in OJAIDocument
*          val partitioner = MapRDBSpark.newPartitioner(tableName)
*          It creates a partitioiner using the splits specified in tableName.
*
*          val partitioner = MapRDBSpark.newPartitioner(Seq("AA","CC"))
*          It creates a partitioner using the splits provided in the sequence.
*          Here three splits will be created (null, "AA") , ("AA","CC") and ("CC", null)
*          Note that this call assumes that user supplies the splits in sorted order.
*/
object MapRDBSpark {

  /**
    * Factory function to convert a ByteBuffer into a serializable binary value.
    * @param buff ByteBuffer
    */
  def serializableBinaryValue(buff: ByteBuffer) : DBBinaryValue = {
    new DBBinaryValue(buff)
  }

  /**
    * Factory function to create a new OJAIDocument from org.ojai.Document.
    * @param doc org.ojai.Document
    */
  def newDocument(doc: Document): OJAIDocument = {
    new OJAIDocument(doc)
  }

  /**
    * Factory function to create a new OJAIDocument from org.ojai.Document.
    */
  def newDocument(): OJAIDocument = {
    new OJAIDocument(DBClient().newDocument())
  }

  /**
    * Factory function to create a new OJAIDocument from a json string.
    * @param jsonString a json document.
    */
  def newDocument(jsonString: String): OJAIDocument = {
    new OJAIDocument(DBClient().newDocument(jsonString))
  }

  /**
    * Factory function to create a new partitioner using existing MapRDBTable.
    * @param table existing tableName in MapRDB
    */
  def newPartitioner[T: OJAIKEY](table: String, bufferWrites: Boolean = true): Partitioner = {
    MapRDBPartitioner(table, bufferWrites)
  }

  /**
    * Factory function to create a new partitioner from splits provided as sequence.
    * @param splits a sequence of splits
    *               Splits supported at this point are String and ByteBuffer.
    * It is user's responsibility to supply the splits in ascending order.
    */
  def newPartitioner[T: OJAIKEY](splits: Seq[T]): MapRDBSplitPartitioner[T] = {
    MapRDBPartitioner[T](splits)
  }

  /**
    * A function to convert a Spark's ROW to OJAIDocument.
    * @param row Spark's Dataframe or Dataset Row.
    */
  def rowToDoc(row: Row) : OJAIDocument = {
    MapRSqlUtils.rowToDocument(row)
  }

  /**
    * A function to convert an OJAI Document to Spark's Row format.
    * @param ojaiDoc OJAI Document to be converted to Spark's Row format.
    * @param schema Schema for the Row.
    */
  def docToRow(ojaiDoc: OJAIDocument, schema : StructType): Row = {
    MapRSqlUtils.documentToRow(ojaiDoc, schema)
  }
}
