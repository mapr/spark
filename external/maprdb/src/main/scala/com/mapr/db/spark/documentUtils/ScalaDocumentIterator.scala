/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.documentUtils

import org.ojai.Value
import java.util.Map

import com.mapr.db.spark.codec.BeanCodec
import com.mapr.db.spark.impl.OJAIDocument
import org.ojai.Document

/*
 * This class is a bridge between the java Map Iterator and scala Map Iterator.
 * It enables users to iterate over java Map.
 */
class ScalaDocumentIterator(val iter: java.util.Iterator[Map.Entry[String, Value]])
  extends Iterator[Tuple2[String,Value]] {

  def hasNext = iter.hasNext

  def next() = {
    val elem = iter.next()
    (elem.getKey -> elem.getValue)
  }
}


class OJAIDocumentIterator(val iter: java.util.Iterator[Document]) extends Iterator[OJAIDocument] {

  def hasNext = iter.hasNext

  def next() = {
    new OJAIDocument(iter.next())
  }
}

class BeanIterator[T](val iter: java.util.Iterator[Document], val beanClass: Class[T]) extends Iterator[T] {

  def hasNext = iter.hasNext

  def next() = {
    BeanCodec.encode[T](iter.next.asReader(), beanClass)
  }
}

case class JavaBeanIterator[T](val iter: java.util.Iterator[Document], val beanClass: Class[T]) extends Iterator[T] {

  def hasNext = iter.hasNext

  def next() = {
    org.ojai.beans.BeanCodec.encode[T](iter.next.asReader(), beanClass)
  }
}