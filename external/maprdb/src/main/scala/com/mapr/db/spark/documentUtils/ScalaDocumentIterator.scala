/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.documentUtils

import com.mapr.db.spark.codec.BeanCodec
import com.mapr.db.spark.impl.OJAIDocument
import java.util
import org.ojai.{Document, Value}

/*
 * This class is a bridge between the java Map Iterator and scala Map Iterator.
 * It enables users to iterate over java Map.
 */
class ScalaDocumentIterator(val iter: java.util.Iterator[util.Map.Entry[String, Value]])
    extends Iterator[(String, Value)] {

  def hasNext: Boolean = iter.hasNext

  def next: (String, Value) = {
    val elem = iter.next()
    elem.getKey -> elem.getValue
  }
}

class OJAIDocumentIterator(val iter: java.util.Iterator[Document])
    extends Iterator[OJAIDocument] {

  def hasNext: Boolean = iter.hasNext

  def next: OJAIDocument = {
    new OJAIDocument(iter.next())
  }
}

class BeanIterator[T](val iter: java.util.Iterator[Document], val beanClass: Class[T])
    extends Iterator[T] {

  def hasNext: Boolean = iter.hasNext

  def next: T = BeanCodec.encode[T](iter.next.asReader(), beanClass)

}

case class JavaBeanIterator[T](iter: java.util.Iterator[Document], beanClass: Class[T])
    extends Iterator[T] {

  def hasNext: Boolean = iter.hasNext

  def next: T = org.ojai.beans.BeanCodec.encode[T](iter.next.asReader(), beanClass)
}
