/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.types

import java.util

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class MapIterator(m: Map[String, AnyRef])
    extends Iterator[(String, AnyRef)] {
  val mapIterator = m.iterator

  def hasNext: Boolean = mapIterator.hasNext

  def next(): (String, AnyRef) = {
    val nextElem = mapIterator.next()
    if (nextElem._2.isInstanceOf[java.util.List[_]]) {
      (nextElem._1, new DBArrayValue(nextElem._2.asInstanceOf[java.util.List[Object]].asScala))
    } else if (nextElem._2.isInstanceOf[java.util.Map[_, _]]) {
      (nextElem._1,
       new DBMapValue(nextElem._2.asInstanceOf[util.Map[String, Object]].asScala.toMap))
    } else {
      nextElem
    }
  }
}

class ListIterator[T](s: Seq[T]) extends Iterator[T] {
  val seqIterator = s.iterator

  def hasNext: Boolean = seqIterator.hasNext

  def next(): T = {
    val nextElem = seqIterator.next()
    if (nextElem.isInstanceOf[java.util.List[_]]) {
      new DBArrayValue(nextElem.asInstanceOf[java.util.List[Object]].asScala)
        .asInstanceOf[T]
    } else if (nextElem.isInstanceOf[java.util.Map[_, _]]) {
      new DBMapValue(
        nextElem.asInstanceOf[util.Map[String, Object]].asScala.toMap)
        .asInstanceOf[T]
    } else {
      nextElem
    }
  }
}