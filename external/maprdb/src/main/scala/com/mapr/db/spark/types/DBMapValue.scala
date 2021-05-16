/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.types

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio._

import scala.collection.JavaConverters._
import scala.collection.MapLike

import com.mapr.db.rowcol.RowcolCodec
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.utils.MapRDBUtils
import com.mapr.db.util.ByteBufs
import java.util

private[spark] final class DBMapValue(
    @transient private[spark] var value: Map[String, AnyRef])
    extends Map[String, AnyRef]
    with MapLike[String, AnyRef, DBMapValue]
    with Externalizable {

  def this() {
    this(null)
  }

  override def empty: DBMapValue = new DBMapValue(Map.empty)

  private lazy val m = Map[String, AnyRef](getMap.toList: _*)

  override def +[B1 >: AnyRef](kv: (String, B1)): Map[String, B1] = m + kv

  override def -(k: String): DBMapValue = new DBMapValue(m - k)

  override def iterator: MapIterator = new MapIterator(m)

  override def get(s: String): Option[AnyRef] = {
    val element = m.get(s)
    if (element.isDefined && element.get.isInstanceOf[java.util.List[_]]) {
      Option(new DBArrayValue(element.get.asInstanceOf[java.util.List[Object]].asScala))
    } else if (element.isDefined && element.get.isInstanceOf[java.util.Map[_, _]]) {
      Option(new DBMapValue(element.get.asInstanceOf[util.Map[String, Object]].asScala.toMap))
    } else {
      element
    }
  }

  private lazy val getMap = value

  override def writeExternal(objectOutput: ObjectOutput): Unit = {
    val newdoc = DBClient()
      .newDocument()
      .set("encode", (value map { case (k, v) => k -> v.asInstanceOf[AnyRef] }).asJava)
    val buff = RowcolCodec.encode(newdoc)
    objectOutput.writeInt(buff.capacity())
    buff.order(ByteOrder.LITTLE_ENDIAN)
    objectOutput.write(buff.array(), 0, buff.capacity())
  }

  override def readExternal(objectinput: ObjectInput): Unit = {
    val buffersize = objectinput.readInt()
    val buffer = ByteBufs.allocate(buffersize)
    MapRDBUtils.readBytes(buffer, buffersize, objectinput)
    this.value = RowcolCodec.decode(buffer).getMap("encode").asScala.toMap
  }

  override def hashCode(): Int = this.keySet.size

  override def equals(other: Any): Boolean = {
    other match {
      case _: Map[_, _] =>
        val that: DBMapValue = new DBMapValue(
          other.asInstanceOf[Map[String, AnyRef]])
        this.getMap == that
      case _ => false
    }
  }
}
