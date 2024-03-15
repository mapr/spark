/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.dbclient

import scala.collection.JavaConverters._

import com.mapr.db.{MapRDB, TableDescriptor}
import com.mapr.db.impl.AdminImpl
import com.mapr.db.scan.ScanRange
import com.mapr.ojai.store.impl.OjaiDocumentStore
import org.ojai.{Document, DocumentBuilder, Value}
import org.ojai.store.{DocumentStore, DriverManager, QueryCondition}

object DBOlderClientImpl extends DBClient {

  private val connection = DriverManager.getConnection("ojai:mapr:")
  private val driver = connection.getDriver
  private val bufferWritesOption = "ojai.mapr.documentstore.buffer-writes"

  override def newDocument(): Document = {
    driver.newDocument()
  }

  override def getTabletInfos(tablePath: String, cond: QueryCondition,
                              bufferWrites: Boolean): Seq[ScanRange] = {

    connection.getStore(tablePath, connection.newDocument()
      .set(bufferWritesOption, bufferWrites))
      .asInstanceOf[OjaiDocumentStore].getTable.getMetaTable
      .getScanRanges(cond).asScala.toSeq
  }

  override def getTabletInfos(tablePath: String, bufferWrites: Boolean): Seq[ScanRange] = {

      connection.getStore(tablePath, connection.newDocument()
        .set(bufferWritesOption, bufferWrites))
        .asInstanceOf[OjaiDocumentStore].getTable.getMetaTable
        .getScanRanges().asScala.toSeq
  }

  override def newDocument(jsonString: String): Document =
    driver.newDocument(jsonString)

  override def newCondition(): QueryCondition = driver.newCondition()

  override def deleteTable(tablePath: String): Unit =
    MapRDB.deleteTable(tablePath)

  override def tableExists(tablePath: String): Boolean =
    MapRDB.tableExists(tablePath)

  override def newTableDescriptor(): TableDescriptor =
    MapRDB.newTableDescriptor()

  override def createTable(tablePath: String): Unit =
    MapRDB.createTable(tablePath)

  override def createTable(tableDesc: TableDescriptor): Unit =
    MapRDB.newAdmin().createTable(tableDesc)

  override def createTable(tableDesc: TableDescriptor, keys: Array[Value]): Unit =
    MapRDB
      .newAdmin()
      .asInstanceOf[AdminImpl]
      .createTable(tableDesc, keys)

  override def isBulkLoad(tablePath: String): Boolean =
    MapRDB
      .newAdmin()
      .getTableDescriptor(tablePath)
      .isBulkLoad

  override def alterTable(tableDesc: TableDescriptor): Unit =
    MapRDB.newAdmin().alterTable(tableDesc)

  override def getTable(tablePath: String, bufferWrites: Boolean): DocumentStore =
    connection.getStore(tablePath, connection.newDocument()
      .set(bufferWritesOption, bufferWrites))

  override def getTableDescriptor(tablePath: String): TableDescriptor =
    MapRDB.newAdmin().getTableDescriptor(tablePath)

  override def getEstimatedSize(scanRange: ScanRange): Long = 0

  override def newDocumentBuilder(): DocumentBuilder =
    driver.newDocumentBuilder()

}
