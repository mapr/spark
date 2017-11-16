/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import com.mapr.db.exceptions.TableNotFoundException
import com.mapr.db.spark.RDD.partitioner.MapRDBPartitioner
import com.mapr.db.spark.condition.{DBQueryCondition, Predicate}
import com.mapr.db.spark.configuration.SerializableConfiguration
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.utils.{LoggingTrait, MapRDBUtils}
import com.mapr.db.spark.writers._

import org.apache.hadoop.conf.Configuration
import org.ojai.{Document, DocumentConstants, Value}
import org.ojai.store.DocumentMutation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

private[spark] class DocumentRDDFunctions extends LoggingTrait {
  protected def saveToMapRDBInternal[T](
      rdd: RDD[T],
      tablename: String,
      createTable: Boolean = false,
      bulkInsert: Boolean = false,
      function1: (Broadcast[SerializableConfiguration],
                  Boolean) => Function1[Iterator[T], Unit]): Unit = {
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
        MapRDBUtils.checkOrCreateTable(tablename, bulkInsert, createTable, keys)
    } catch {
      case e: TableNotFoundException =>
        logError(
          "Table: " + tablename + " not found and createTable set to: " + createTable)
        throw e
      case any: Exception => throw any
    }

    val hadoopConf = new Configuration()
    val serializableConf = new SerializableConfiguration(hadoopConf)
    val cnf: Broadcast[SerializableConfiguration] =
      rdd.context.broadcast(serializableConf)
    rdd.foreachPartition(function1(cnf, isNewAndBulkLoad._2))
    if (isNewAndBulkLoad._1 && isNewAndBulkLoad._2) {
      MapRDBUtils.setBulkLoad(tablename, false)
    }
  }
}

private[spark] case class OJAIDocumentRDDFunctions[T](rdd: RDD[T])(
    implicit f: OJAIValue[T])
    extends DocumentRDDFunctions {

  @transient val sparkContext = rdd.sparkContext

  def saveToMapRDB(tablename: String,
                   createTable: Boolean = false,
                   bulkInsert: Boolean = false,
                   idFieldPath: String = DocumentConstants.ID_KEY): Unit = {
    logDebug(
      s"saveToMapRDB in OJAIDocumentRDDFunctions is called for table: $tablename " +
        s"with bulkinsert flag set: $bulkInsert and createTable: $createTable")

    var getID: (Document) => Value = null
    if (idFieldPath == DocumentConstants.ID_KEY) {
      getID = (doc: Document) => doc.getId
    } else {
      getID = (doc: Document) => doc.getValue(idFieldPath)
    }

    this.saveToMapRDBInternal(
      rdd,
      tablename,
      createTable,
      bulkInsert,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[T]) => {
          if (iter.nonEmpty) {
            val writer =
              Writer.initialize(tablename, cnf.value, isnewAndBulkLoad, true)
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

    var getID: (Document) => Value = if (idFieldPath == DocumentConstants.ID_KEY) {
     (doc: Document) => doc.getId
    } else {
      (doc: Document) => doc.getValue(idFieldPath)
    }

    this.saveToMapRDBInternal(
      rdd,
      tablename,
      createTable,
      bulkInsert,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[T]) => {
          if (iter.nonEmpty) {
            val writer =
              Writer.initialize(tablename, cnf.value, isnewAndBulkLoad, false)
            while (iter.hasNext) {
              val element = iter.next
              f.write(f.getValue(element), getID, writer)
            }
            writer.close()
          }
      }
    )
  }

  def updateToMapRDB(tablename: String,
                     mutation: (T) => DocumentMutation,
                     getID: (T) => Value): Unit = {
    logDebug(
      "updateToMapRDB in OJAIDocumentRDDFunctions is called for table: " + tablename)
    this.saveToMapRDBInternal(
      rdd,
      tablename,
      false,
      false,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[T]) =>
          if (iter.nonEmpty) {
            val writer = TableUpdateWriter(DBClient().getTable(tablename))
            while (iter.hasNext) {
              val element = iter.next
              f.update(mutation(element), getID(element), writer)
            }
            writer.close()
      }
    )
  }

  def updateToMapRDB(tablename: String,
                     mutation: (T) => DocumentMutation,
                     getID: (T) => Value,
                     condition: Predicate): Unit = {
    logDebug(
      "updateToMapRDB in OJAIDocumentRDDFunctions is called for table: " + tablename)
    val queryCondition = DBQueryCondition(condition.build.build())

    this.saveToMapRDBInternal(
      rdd,
      tablename,
      false,
      false,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[T]) =>
          if (iter.nonEmpty) {
            val writer =
              TableCheckAndMutateWriter(DBClient().getTable(tablename))
            while (iter.hasNext) {
              val element = iter.next
              f.checkAndUpdate(mutation(element),
                               queryCondition,
                               getID(element),
                               writer)
            }
            writer.close()
      }
    )
  }
}

private[spark] case class PairedDocumentRDDFunctions[K, V](rdd: RDD[(K, V)])(
    implicit f: OJAIKey[K],
    v: OJAIValue[V])
    extends DocumentRDDFunctions {

  @transient val sparkContext = rdd.sparkContext
  def saveToMapRDB(tablename: String,
                   createTable: Boolean = false,
                   bulkInsert: Boolean = false): Unit = {
    logDebug(
      "saveToMapRDB in PairedDocumentRDDFunctions is called for table: " +
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
              Writer.initialize(tablename, cnf.value, isnewAndBulkLoad, true)
            while (iter.hasNext) {
              val element = iter.next
              f.write(v.getValue(element._2), f.getValue(element._1), writer)
            }
            writer.close()
      }
    )
  }

  def insertToMapRDB(tablename: String,
                     createTable: Boolean = false,
                     bulkInsert: Boolean = false): Unit = {
    logDebug(
      "insertToMapRDB in PairedDocumentRDDFunctions is called for table: " +
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
              Writer.initialize(tablename, cnf.value, isnewAndBulkLoad, false)
            while (iter.hasNext) {
              val element = iter.next
              f.write(v.getValue(element._2), f.getValue(element._1), writer)
            }
            writer.close()
      }
    )
  }

  def updateToMapRDB(tablename: String,
                     mutation: (V) => DocumentMutation): Unit = {
    logDebug(
      "updateToMapRDB in PairedDocumentRDDFunctions is called for table: " + tablename)

    this.saveToMapRDBInternal[(K, V)](
      rdd,
      tablename,
      false,
      false,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[(K, V)]) =>
          if (iter.nonEmpty) {
            val writer = TableUpdateWriter(DBClient().getTable(tablename))
            while (iter.hasNext) {
              val element = iter.next
              f.update(mutation(element._2), f.getValue(element._1), writer)
            }
            writer.close()
      }
    )
  }

  def updateToMapRDB(tablename: String,
                     mutation: (V) => DocumentMutation,
                     condition: Predicate): Unit = {
    logDebug(
      "updateToMapRDB in PairedDocumentRDDFunctions is called for table: " + tablename)

    val queryCondition = DBQueryCondition(condition.build.build())

    this.saveToMapRDBInternal[(K, V)](
      rdd,
      tablename,
      false,
      false,
      (cnf: Broadcast[SerializableConfiguration], isnewAndBulkLoad: Boolean) =>
        (iter: Iterator[(K, V)]) =>
          if (iter.nonEmpty) {
            val writer =
              TableCheckAndMutateWriter(DBClient().getTable(tablename))
            while (iter.hasNext) {
              val element = iter.next
              f.checkAndMutate(mutation(element._2),
                               queryCondition,
                               f.getValue(element._1),
                               writer)
            }
            writer.close()
      }
    )
  }
}
