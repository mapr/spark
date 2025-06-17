/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.impl

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import com.mapr.db.spark.documentTypeUtils.OJAIType
import com.mapr.db.spark.types.DBBinaryValue
import com.mapr.db.spark.utils.DefaultClass.DefaultType
import org.ojai.types._
import scala.language.dynamics

/**
* OJAIDocument represents a JSON document which can be accessed with dynamic object model
*   as well as through setters and getters.
* OJAIDocument extends the original org.ojai.Document with input and output types mapped
*   to that of scala types.
* To create an OJAIDocument use the factory function newDocument in MapRDBSpark or through
*   sparkContext functions loadMapRDBTable.
* @constructor Create a new scala's version of ojai Document from java's version of ojai
*              document.
* @param dc java's version of ojai Document
* @example val doc = MapRDBSpark.newDocument(jsonString) or
*          val docs = sc.loadMapRDBTable(tableName)
*
*          Here are the ways to access elements in OJAIDocument
*          doc.address -- accesses the element with address as the key and returns the value
*          specified for it as AnyRef
*          doc.address[String] -- access the elements with address as the key and returns
*          the value specified for it as String
*          doc.getString("address") -- getString is an accessor method that can also be used
*          to get the value for address as String.
*
*          Here are the ways to set value to a key in OJAIDocument
*          doc.city = "San Francisco" or doc.set("city", "San Francisco")
*/
class OJAIDocument(@transient private var dc: org.ojai.Document)
    extends ScalaOjaiDocument[OJAIDocument](dc)
    with Dynamic {

  override def THIS: OJAIDocument = this

  private[spark] def this() = {
    this(null)
  }

  // Getter functionality of the dynamic object model is provided by the following function.
  def selectDynamic[T](fieldPath: String)
                      (implicit e: T DefaultType AnyRef, f: OJAIType[T]): f.Self = {
    f.getValue(this.getDoc, fieldPath)
  }

  // Setter functionality of the dynamic object model is provided by the following function.
  def updateDynamic[T](fieldPath: String)(v: T): Unit = {
    v match {
      case null => getDoc.setNull(fieldPath)
      case str: String => getDoc.set(fieldPath, str)
      case date: ODate => getDoc.set(fieldPath, date)
      case integer: Integer => getDoc.set(fieldPath, integer)
      case b: Byte => getDoc.set(fieldPath, b)
      case sh: Short => getDoc.set(fieldPath, sh)
      case bool: Boolean => getDoc.set(fieldPath, bool)
      case decimal: BigDecimal => getDoc.set(fieldPath, decimal.bigDecimal)
      case d: Double => getDoc.set(fieldPath, d)
      case fl: Float => getDoc.set(fieldPath, fl)
      case interval: OInterval => getDoc.set(fieldPath, interval)
      case l: Long => getDoc.set(fieldPath, l)
      case time: OTime => getDoc.set(fieldPath, time)
      case timestamp: OTimestamp => getDoc.set(fieldPath, timestamp)
      case _: Map[_, _] => getDoc.set(fieldPath, v.asInstanceOf[Map[String, AnyRef]].asJava)
      case _: Seq[Any] => getDoc.set(fieldPath, v.asInstanceOf[Seq[AnyRef]].asJava)
//      case value: DBMapValue => getDoc.set(fieldPath, value
//            .map({ case (k, v1) => k -> v1.asInstanceOf[AnyRef] })
//            .asJava)

//      case _: DBArrayValue[_] =>
//        getDoc.set(fieldPath, v.asInstanceOf[DBArrayValue[AnyRef]].arr.asJava)

      case value: DBBinaryValue => getDoc.set(fieldPath, value.getByteBuffer())
      case buffer: ByteBuffer => getDoc.set(fieldPath, buffer)
      case bytes: Array[Byte] => getDoc.set(fieldPath, bytes)
      case _ => throw new RuntimeException("Not valid value to set")
    }
  }
}
