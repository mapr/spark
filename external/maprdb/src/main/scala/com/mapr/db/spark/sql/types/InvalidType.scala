/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql.types

import org.apache.spark.sql.types.DataType

private[sql] class InvalidType private () extends DataType with Serializable {
  def defaultSize: Int = 0

  def asNullable: DataType = this
  override def toString: String = "InvalidType"
}

private[sql] case object InvalidType extends InvalidType
