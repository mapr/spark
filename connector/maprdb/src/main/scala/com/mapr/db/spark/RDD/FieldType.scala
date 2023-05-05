/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import org.ojai.FieldPath

trait FIELD[T] extends Serializable {
  def getFields(fields: Seq[Any]): Seq[String]
}

object FIELD {
  implicit val fieldStrings = new FIELD[String] {
    def getFields(fields: Seq[Any]) = fields.asInstanceOf[Seq[String]]
  }

  implicit val fieldPaths = new FIELD[FieldPath] {
    def getFields(fields: Seq[Any]) =
      fields.map(field => field.asInstanceOf[FieldPath].asPathString())
  }
}
