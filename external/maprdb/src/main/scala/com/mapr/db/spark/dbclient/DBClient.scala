/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.db.spark.dbclient

import com.mapr.db.{TableDescriptor, TabletInfo}
import org.ojai.{Document, DocumentBuilder, Value}
import org.ojai.store.{DocumentStore, QueryCondition}

trait DBClient {
  def newDocument() : Document
  def newDocument(jsonString: String): Document
  def newCondition(): QueryCondition
  def deleteTable(tablePath: String): Unit
  def tableExists(tablePath: String): Boolean
  def newTableDescriptor(): TableDescriptor
  def createTable(tablePath: String): Unit
  def createTable(tableDesc: TableDescriptor): Unit
  def createTable(tableDesc: TableDescriptor, keys: Array[Value]): Unit
  def isBulkLoad(tablePath: String): Boolean
  def alterTable(tableDesc: TableDescriptor): Unit
  def getTable(tablePath: String): DocumentStore
  def getTableDescriptor(tablePath: String): TableDescriptor
  def newDocumentBuilder(): DocumentBuilder
  def getTabletInfos(tablePath: String, cond: QueryCondition): Seq[TabletInfo]
  def getTabletInfos(tablePath: String): Seq[TabletInfo]
  def getEstimatedSize(scanRange: TabletInfo) : Long
}

object DBClient {
  val CLIENT_VERSION = "1.0"

  val newClient = DBOlderClientImpl
  val oldClient = DBOlderClientImpl

  def apply(): DBClient = {
    if (CLIENT_VERSION.equals("2.0")) newClient else oldClient
  }
}
