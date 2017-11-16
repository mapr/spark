/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.dbclient

import com.mapr.db.{MapRDB, Table, TableDescriptor, TabletInfo}
import com.mapr.db.impl.AdminImpl
import org.ojai.{Document, DocumentBuilder, Value}
import org.ojai.store.QueryCondition

object DBOlderClientImpl extends DBClient {

  override def newDocument(): Document = {
    MapRDB.newDocument()
  }

  override def getTabletInfos(tablePath: String, cond: QueryCondition): Seq[TabletInfo] = {
    MapRDB.getTable(tablePath).getTabletInfos(cond)
  }

  override def getTabletInfos(tablePath: String): Seq[TabletInfo] = {
    MapRDB.getTable(tablePath).getTabletInfos
  }

  override def newDocument(jsonString: String): Document =
    MapRDB.newDocument(jsonString)

  override def newCondition(): QueryCondition = MapRDB.newCondition()

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

  override def getTable(tablePath: String): Table = MapRDB.getTable(tablePath)

  override def getTableDescriptor(tablePath: String): TableDescriptor =
    MapRDB.newAdmin().getTableDescriptor(tablePath)

  override def getEstimatedSize(scanRange: TabletInfo): Long = 0

  override def newDocumentBuilder(): DocumentBuilder =
    MapRDB.newDocumentBuilder()

}
