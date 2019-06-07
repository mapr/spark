/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.db.spark.dbclient

import com.mapr.db.TableDescriptor
import com.mapr.db.scan.ScanRange
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
  def getTable(tablePath: String, bufferWrites: Boolean): DocumentStore
  def getTableDescriptor(tablePath: String): TableDescriptor
  def newDocumentBuilder(): DocumentBuilder
  def getTabletInfos(tablePath: String, cond: QueryCondition,
                     bufferWrites: Boolean): Seq[ScanRange]
  def getTabletInfos(tablePath: String, bufferWrites: Boolean): Seq[ScanRange]
  def getEstimatedSize(scanRange: ScanRange) : Long
}

object DBClient {
  val CLIENT_VERSION = "1.0"

  private val newClient = DBOlderClientImpl
  private val oldClient = DBOlderClientImpl

  def apply(): DBClient = {
    if (CLIENT_VERSION.equals("2.0")) newClient else oldClient
  }
}
