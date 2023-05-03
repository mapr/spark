/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.writers

import com.mapr.db.spark.codec.BeanCodec
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.sql.utils.MapRSqlUtils
import org.ojai.{Document, Value}

import org.apache.spark.sql.Row

private[spark] sealed trait OJAIValue[T] extends Serializable {
  type Self
  def getValue(elem: T): Document
  def write(doc: Document, getID: (Document) => Value, writer: Writer)
}

private[spark] object OJAIValue extends BaseOJAIValue {

  implicit def rowOJAIDocument[T]: OJAIValue[Row] = new OJAIValue[Row] {
    override type Self = Row

    override def getValue(elem: Row): Document =
      MapRSqlUtils.rowToDocument(elem).getDoc

    override def write(doc: Document,
                       getID: (Document) => Value,
                       writer: Writer) = writer.write(doc, getID(doc))
  }

  implicit def defaultOJAIDocument[T]: OJAIValue[OJAIDocument] =
    new OJAIValue[OJAIDocument] {
      type Self = OJAIDocument
      override def getValue(elem: OJAIDocument): Document = elem.getDoc
      override def write(doc: Document,
                         getID: (Document) => Value,
                         writer: Writer) = writer.write(doc, getID(doc))
    }
}

private[spark] trait BaseOJAIValue {
  implicit def overrideDefault[T <: AnyRef]: OJAIValue[T] = new OJAIValue[T] {
    type Self = AnyRef
    override def getValue(elem: T): Document =
      BeanCodec.decode(DBClient().newDocumentBuilder(), elem)
    override def write(doc: Document,
                       getID: (Document) => Value,
                       writer: Writer) = writer.write(doc, getID(doc))
  }

  def overrideJavaDefault[T <: AnyRef]: OJAIValue[T] = new OJAIValue[T] {
    type Self = AnyRef
    override def getValue(elem: T): Document =
      org.ojai.beans.BeanCodec.decode(DBClient().newDocumentBuilder(), elem)
    override def write(doc: Document,
                       getID: (Document) => Value,
                       writer: Writer) = writer.write(doc, getID(doc))
  }
}
