/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.types

import scala.collection.JavaConverters._

import java.util

class MapIterator(m: Map[String, AnyRef])
    extends Iterator[(String, AnyRef)] {
  val mapIterator = m.iterator

  def hasNext: Boolean = mapIterator.hasNext

  def next(): (String, AnyRef) = {
    val nextElem = mapIterator.next()
    nextElem._2 match {
      case _: util.List[_] =>
        (nextElem._1, new DBArrayValue(nextElem._2.asInstanceOf[util.List[Object]].asScala.toSeq))
      case _: util.Map[_, _] =>
        (nextElem._1,
          new DBMapValue(nextElem._2.asInstanceOf[util.Map[String, Object]].asScala.toMap))
      case _ =>
        nextElem
    }
  }
}

class ListIterator[T](s: Seq[T]) extends Iterator[T] {
  val seqIterator = s.iterator

  def hasNext: Boolean = seqIterator.hasNext

  def next(): T = {
    val nextElem = seqIterator.next()
    nextElem match {
      case _: util.List[_] =>
        new DBArrayValue(nextElem.asInstanceOf[util.List[Object]].asScala.toSeq)
          .asInstanceOf[T]
      case _: util.Map[_, _] =>
        new DBMapValue(
          nextElem.asInstanceOf[util.Map[String, Object]].asScala.toMap)
          .asInstanceOf[T]
      case _ =>
        nextElem
    }
  }
}
