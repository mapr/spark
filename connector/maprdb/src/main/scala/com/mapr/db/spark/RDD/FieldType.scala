/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import scala.collection.{Seq => CSeq}

import org.ojai.FieldPath

trait FIELD[T] extends Serializable {
  def getFields(fields: CSeq[Any]): Seq[String]
}

object FIELD {
  implicit val fieldStrings: FIELD[String] = new FIELD[String] {
    def getFields(fields: CSeq[Any]) = fields.map(_.asInstanceOf[String]).toSeq
  }

  implicit val fieldPaths: FIELD[FieldPath] = new FIELD[FieldPath] {
    def getFields(fields: CSeq[Any]) =
      fields.map(field => field.asInstanceOf[FieldPath].asPathString()).toSeq
  }
}
