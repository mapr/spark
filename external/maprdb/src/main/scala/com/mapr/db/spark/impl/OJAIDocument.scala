/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.impl

import java.nio.ByteBuffer

import com.mapr.db.spark.documentTypeUtils.OJAIType
import com.mapr.db.spark.types.{DBArrayValue, DBBinaryValue, DBMapValue}
import com.mapr.db.spark.utils.DefaultClass.DefaultType
import org.ojai.types._

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.language.{dynamics, implicitConversions}

/**
  * OJAIDocument represents a JSON document which can be accessed with dynamic object model as well as through setters and getters.
  * OJAIDocument extends the original org.ojai.Document with input and output types mapped to that of scala types.
  * To create an OJAIDocument use the factory function newDocument in MapRDBSpark or through sparkContext functions loadMapRDBTable.
  * @constructor Create a new scala's version of ojai Document from java's version of ojai document.
  * @param dc java's version of ojai Document
  * @example val doc = MapRDBSpark.newDocument(jsonString) or val docs = sc.loadMapRDBTable(tableName)
  *
  *          Here are the ways to access elements in OJAIDocument
  *          doc.address -- accesses the element with address as the key and returns the value specified for it as AnyRef
  *          doc.address[String] -- access the elements with address as the key and returns the value specified for it as String
  *          doc.getString("address") -- getString is an accessor method that can also be used to get the value for address as String.
  *
  *          Here are the ways to set value to a key in OJAIDocument
  *          doc.city = "San Francisco" or doc.set("city", "San Francisco")
  */
class OJAIDocument(@transient private var dc: org.ojai.Document)
                        extends ScalaOjaiDocument[OJAIDocument](dc) with Dynamic{

  override def THIS: OJAIDocument = this

  private[spark] def this() {
    this(null)
  }
   //Getter functionality of the dynamic object model is provided by the following function.
  def selectDynamic[T](fieldPath: String)(implicit e: T DefaultType AnyRef, f : OJAIType[T]): f.Self = {
    f.getValue(this.getDoc, fieldPath)
  }

  //Setter functionality of the dynamic object model is provided by the following function.
  def updateDynamic[T <: Any](fieldPath: String)(v: T) : Unit = {
    if (v == null) {
      getDoc.setNull(fieldPath)
      return
    }
    if (v.isInstanceOf[String])
      getDoc.set(fieldPath, v.asInstanceOf[String])
    else if (v.isInstanceOf[ODate])
      getDoc.set(fieldPath, v.asInstanceOf[ODate])
    else if (v.isInstanceOf[Integer])
      getDoc.set(fieldPath, v.asInstanceOf[Integer])
    else if (v.isInstanceOf[Byte])
      getDoc.set(fieldPath, v.asInstanceOf[Byte])
    else if (v.isInstanceOf[Short])
      getDoc.set(fieldPath, v.asInstanceOf[Short])
    else if (v.isInstanceOf[Boolean])
      getDoc.set(fieldPath, v.asInstanceOf[Boolean])
    else if (v.isInstanceOf[BigDecimal])
      getDoc.set(fieldPath, v.asInstanceOf[BigDecimal].bigDecimal)
    else if (v.isInstanceOf[Double])
      getDoc.set(fieldPath, v.asInstanceOf[Double])
    else if (v.isInstanceOf[Float])
      getDoc.set(fieldPath, v.asInstanceOf[Float])
    else if (v.isInstanceOf[OInterval])
      getDoc.set(fieldPath, v.asInstanceOf[OInterval])
    else if (v.isInstanceOf[Long])
      getDoc.set(fieldPath, v.asInstanceOf[Long])
    else if (v.isInstanceOf[OTime])
      getDoc.set(fieldPath, v.asInstanceOf[OTime])
    else if (v.isInstanceOf[OTimestamp])
      getDoc.set(fieldPath, v.asInstanceOf[OTimestamp])
    else if (v.isInstanceOf[Map[_, _]])
      getDoc.set(fieldPath, v.asInstanceOf[Map[String,AnyRef]].asJava)
    else if (v.isInstanceOf[Seq[Any]])
      getDoc.set(fieldPath, v.asInstanceOf[Seq[AnyRef]].asJava)
    else if (v.isInstanceOf[DBMapValue])
      getDoc.set(fieldPath, v.asInstanceOf[DBMapValue].map({ case (k,v) => k -> v.asInstanceOf[AnyRef]}).asJava)
    else if (v.isInstanceOf[DBArrayValue[_]])
      getDoc.set(fieldPath, v.asInstanceOf[DBArrayValue[AnyRef]].arr.asJava)
    else if (v.isInstanceOf[DBBinaryValue])
      getDoc.set(fieldPath, v.asInstanceOf[DBBinaryValue].getByteBuffer())
    else if (v.isInstanceOf[ByteBuffer])
      getDoc.set(fieldPath, v.asInstanceOf[ByteBuffer])
    else if (v.isInstanceOf[Array[Byte]])
      getDoc.set(fieldPath, v.asInstanceOf[Array[Byte]])
    else
      throw new RuntimeException("Not valid value to set")
  }
}