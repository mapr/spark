/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.dbclient

import com.mapr.db.{TableDescriptor, TabletInfo, MapRDB}
import com.mapr.db.impl.AdminImpl
import com.mapr.db.TabletInfo
import org.ojai.store.QueryCondition
import org.ojai.{Document, Value}

object DBOlderClientImpl extends DBClient {
  @Override
  def newDocument(): Document = {
    MapRDB.newDocument()
  }

  @Override
  override def getTabletInfos(tablePath: String, cond: QueryCondition): Seq[TabletInfo] = {
    MapRDB.getTable(tablePath).getTabletInfos(cond)
  }

  @Override
  override def getTabletInfos(tablePath: String): Seq[TabletInfo] = {
    MapRDB.getTable(tablePath).getTabletInfos
  }

  @Override
  def newDocument(jsonString: String) = {
    MapRDB.newDocument(jsonString)
  }

  @Override
  def newCondition() = {
    MapRDB.newCondition()
  }

  @Override
  def deleteTable(tablePath: String) = {
    MapRDB.deleteTable(tablePath)
  }

  @Override
  def tableExists(tablePath: String) = {
    MapRDB.tableExists(tablePath)
  }

  @Override
  def newTableDescriptor() = {
    MapRDB.newTableDescriptor()
  }

  @Override
  def createTable(tablePath: String) = {
    MapRDB.createTable(tablePath)
  }

  @Override
  def createTable(tableDesc: TableDescriptor) = {
    MapRDB.newAdmin().createTable(tableDesc)
  }

  @Override
  def createTable(tableDesc: TableDescriptor, keys: Array[Value]) = {
    MapRDB.newAdmin().asInstanceOf[AdminImpl].createTable(tableDesc, keys)
  }

  @Override
  def isBulkLoad(tablePath: String) = {
    MapRDB.newAdmin().getTableDescriptor(tablePath).isBulkLoad
  }

  @Override
  def alterTable(tableDesc: TableDescriptor) = {
    MapRDB.newAdmin().alterTable(tableDesc)
  }

  @Override
  def getTable(tablePath: String) = {
    MapRDB.getTable(tablePath)
  }

  @Override
  def getTableDescriptor(tablePath: String) = {
    MapRDB.newAdmin().getTableDescriptor(tablePath)
  }

  @Override
  override def getEstimatedSize(scanRange: TabletInfo): Long = {
    return 0
  }

  @Override
  def newDocumentBuilder() = {
    MapRDB.newDocumentBuilder()
  }
}
