/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import com.mapr.db.exceptions.TableNotFoundException
import com.mapr.db.spark.RDD.partitioner.MapRDBPartitioner
import com.mapr.db.spark.configuration.SerializableConfiguration
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.utils.{LoggingTrait, MapRDBUtils}
import com.mapr.db.spark.writers._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Partitioner
import org.ojai.{Document, DocumentConstants, Value}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

private[spark] class DocumentRDDFunctions extends LoggingTrait {
  protected def saveToMapRDBInternal[T](
                 rdd: RDD[T],
                 tableName: String,
                 createTable: Boolean = false,
                 bulkInsert: Boolean = false,
                 function1: (Broadcast[SerializableConfiguration], Boolean) =>
                   ((Iterator[T]) => Unit)): Unit = {

    var isNewAndBulkLoad = (false, false)

    val partitioner: Option[Partitioner] = rdd.partitioner
    val keys: Seq[Value] =
      if (partitioner.isDefined && partitioner.get
            .isInstanceOf[MapRDBPartitioner]) {
        logDebug(
          "RDD's partitioner is being used to create the table" + partitioner)
        partitioner.get.asInstanceOf[MapRDBPartitioner].splits
      } else {
        logDebug("it has no partitioner")
        Seq.empty
      }

    try {
      isNewAndBulkLoad =
        MapRDBUtils.checkOrCreateTable(tableName, bulkInsert, createTable, keys)
    } catch {
      case e: TableNotFoundException =>
        logError("Table: " + tableName + " not found and createTable set to: " + createTable)
        throw e
      case any: Exception => throw any
    }

    val hadoopConf = new Configuration()
    val serializableConf = new SerializableConfiguration(hadoopConf)
    val cnf: Broadcast[SerializableConfiguration] =
      rdd.context.broadcast(serializableConf)
    rdd.foreachPartition(function1(cnf, isNewAndBulkLoad._2))
    if (isNewAndBulkLoad._1 && isNewAndBulkLoad._2) {
      MapRDBUtils.setBulkLoad(tableName, false)
    }
  }

  protected def deleteFromMapRDBInternal[T](
                 rdd: RDD[T],
                 function1: (Broadcast[SerializableConfiguration], Boolean) =>
                   ((Iterator[T]) => Unit)): Unit = {
    val hadoopConf = new Configuration()
    val serializableConf = new SerializableConfiguration(hadoopConf)
    val cnf: Broadcast[SerializableConfiguration] =
      rdd.context.broadcast(serializableConf)
    rdd.foreachPartition(function1(cnf, false))
  }
}

private[spark] case class OJAIDocumentRDDFunctions[T](rdd: RDD[T], bufferWrites: Boolean = true)(
    implicit f: OJAIValue[T])
    extends DocumentRDDFunctions {

  @transient val sparkContext = rdd.sparkContext

  def setBufferWrites(bufferWrites: Boolean): OJAIDocumentRDDFunctions[T] =
    OJAIDocumentRDDFunctions(rdd, bufferWrites)

  def saveToMapRDB(tableName: String,
                   createTable: Boolean = false,
                   bulkInsert: Boolean = false,
                   idFieldPath: String = DocumentConstants.ID_KEY): Unit = {
    logDebug(
      s"saveToMapRDB in OJAIDocumentRDDFunctions is called for table: $tableName " +
        s"with bulkinsert flag set: $bulkInsert and createTable: $createTable")

    val getID: Document => Value = if (idFieldPath == DocumentConstants.ID_KEY) {
      (doc: Document) => doc.getId
    } else {
      (doc: Document) => doc.getValue(idFieldPath)
    }

    this.saveToMapRDBInternal(
      rdd,
      tableName,
      createTable,
      bulkInsert,
      (cnf: Broadcast[SerializableConfiguration], isNewAndBulkLoad: Boolean) =>
        (iter: Iterator[T]) => {
          if (iter.nonEmpty) {
            val writer =
              Writer.initialize(tableName, cnf.value, isNewAndBulkLoad, true, bufferWrites)
            while (iter.hasNext) {
              val element = iter.next
              f.write(f.getValue(element), getID, writer)
            }
            writer.close()
          }
      }
    )
  }

  def insertToMapRDB(tablename: String,
                     createTable: Boolean = false,
                     bulkInsert: Boolean = false,
                     idFieldPath: String = DocumentConstants.ID_KEY): Unit = {
    logDebug(
      s"insertToMapRDB in OJAIDocumentRDDFunctions is called for table: $tablename" +
        s" with bulkinsert flag set: $bulkInsert and createTable: $createTable")

    val getID: (Document) => Value = if (idFieldPath == DocumentConstants.ID_KEY) {
      (doc: Document) => doc.getId
    } else {
      (doc: Document) => doc.getValue(idFieldPath)
    }

    this.saveToMapRDBInternal(
      rdd,
      tablename,
      createTable,
      bulkInsert,
      (cnf: Broadcast[SerializableConfiguration], isNewAndBulkLoad: Boolean) =>
        (iter: Iterator[T]) => {
          if (iter.nonEmpty) {
            val writer =
              Writer.initialize(tablename, cnf.value, isNewAndBulkLoad, false, bufferWrites)
            while (iter.hasNext) {
              val element = iter.next
              f.write(f.getValue(element), getID, writer)
            }
            writer.close()
          }
      }
    )
  }

  def deleteFromMapRDB(tablename: String,
                       idFieldPath: String = DocumentConstants.ID_KEY): Unit = {
    logDebug(
      s"deleteFromMapRDB in OJAIDocumentRDDFunctions is called for table: $tablename")

    val getID: (Document) => Value = if (idFieldPath == DocumentConstants.ID_KEY) {
      (doc: Document) => doc.getId
    } else {
      (doc: Document) => doc.getValue(idFieldPath)
    }

    this.deleteFromMapRDBInternal(
      rdd,
      (cnf: Broadcast[SerializableConfiguration], isNewAndBulkLoad: Boolean) =>
        (iter: Iterator[T]) => {
          if (iter.nonEmpty) {
            val writer =
              TableDeleteWriter(DBClient().getTable(tablename, bufferWrites))
            while (iter.hasNext) {
              val element = iter.next
              f.write(f.getValue(element), getID, writer)
            }
            writer.close()
          }
        }
    )
  }
}

private[spark] case class PairedDocumentRDDFunctions[K, V](rdd: RDD[(K, V)],
                                                           bufferWrites: Boolean = true)(
    implicit f: OJAIKey[K],
    v: OJAIValue[V])
    extends DocumentRDDFunctions {

  @transient val sparkContext = rdd.sparkContext

  def setBufferWrites(bufferWrites: Boolean): PairedDocumentRDDFunctions[K, V] =
    PairedDocumentRDDFunctions(rdd, bufferWrites)

  def saveToMapRDB(tableName: String,
                   createTable: Boolean = false,
                   bulkInsert: Boolean = false): Unit = {
    logDebug(
      "saveToMapRDB in PairedDocumentRDDFunctions is called for table: " +
        tableName + " with bulkinsert flag set: " + bulkInsert + " and createTable:" + createTable)

    this.saveToMapRDBInternal[(K, V)](
      rdd,
      tableName,
      createTable,
      bulkInsert,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[(K, V)]) =>
          if (iter.nonEmpty) {
            val writer =
              Writer.initialize(tableName, cnf.value, isnewAndBulkLoad, true, bufferWrites)
            while (iter.hasNext) {
              val element = iter.next
              checkElementForNull(element)
              f.write(v.getValue(element._2), f.getValue(element._1), writer)
            }
            writer.close()
      }
    )
  }

  def insertToMapRDB(tablename: String,
                     createTable: Boolean = false,
                     bulkInsert: Boolean = false): Unit = {

    logDebug("insertToMapRDB in PairedDocumentRDDFunctions is called for table: " +
        tablename + " with bulkinsert flag set: " + bulkInsert + " and createTable:" + createTable)

    this.saveToMapRDBInternal[(K, V)](
      rdd,
      tablename,
      createTable,
      bulkInsert,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[(K, V)]) =>
          if (iter.nonEmpty) {
            val writer =
              Writer.initialize(tablename, cnf.value, isnewAndBulkLoad, false, bufferWrites)
            while (iter.hasNext) {
              val element = iter.next
              checkElementForNull(element)
              f.write(v.getValue(element._2), f.getValue(element._1), writer)
            }
            writer.close()
      }
    )
  }

  def deleteFromMapRDB(tablename: String): Unit = {

    logDebug("deleteFromMapRDB in PairedDocumentRDDFunctions is called for table: " +
      tablename)

    this.deleteFromMapRDBInternal[(K, V)](
      rdd,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[(K, V)]) =>
          if (iter.nonEmpty) {
            val writer =
              TableDeleteWriter(DBClient().getTable(tablename, bufferWrites))
            while (iter.hasNext) {
              val element = iter.next
              checkElementForNull(element)
              f.write(v.getValue(element._2), f.getValue(element._1), writer)
            }
            writer.close()
          }
    )
  }

  def checkElementForNull(element: (K, V)): Boolean = {
    if (element._1 == null || element._2 == null) {
      throw  new IllegalArgumentException("Key/Value cannot be null")
    }
    true
  }
}
