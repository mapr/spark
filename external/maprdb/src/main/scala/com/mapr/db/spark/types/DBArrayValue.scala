/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.types

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio._
import java.util

import scala.collection.JavaConverters._
import scala.collection.generic.{CanBuildFrom, GenericTraversableTemplate, SeqFactory}
import scala.collection.mutable.ListBuffer
import scala.collection.SeqLike
import scala.language.implicitConversions
import com.mapr.db.rowcol.RowcolCodec
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.utils.MapRDBUtils
import com.mapr.db.util.ByteBufs

private[spark] object DBArrayValue extends SeqFactory[DBArrayValue] {
  implicit def canBuildFrom[T]: CanBuildFrom[Coll, T, DBArrayValue[T]] =
    new GenericCanBuildFrom[T]
  def newBuilder[T] = new ListBuffer[T] mapResult (x => new DBArrayValue(x))
}

private[spark] class DBArrayValue[T]( @transient private[spark] var arr : Seq[T])
  extends Seq[T]
    with GenericTraversableTemplate[T, DBArrayValue]
    with SeqLike[T, DBArrayValue[T]] with Externalizable{

  def this() {
    this(null)
  }

  override def companion = DBArrayValue
  def iterator: Iterator[T] = new ListIterator[T](arr)
  def apply(idx: Int): T = {
    if (idx < 0 || idx>=length) throw new IndexOutOfBoundsException
    val element = arr(idx)
    if (element.isInstanceOf[java.util.List[_]]) {
      new DBArrayValue(element.asInstanceOf[java.util.List[Object]].asScala).asInstanceOf[T]
    } else if (element.isInstanceOf[java.util.Map[_, _]]) {
      new DBMapValue(element.asInstanceOf[util.Map[String, Object]].asScala.toMap).asInstanceOf[T]
    } else
      element
  }

  def length: Int = arr.size

  private def getval = this.arr

  override def writeExternal(objectOutput: ObjectOutput): Unit = {
    val newdoc = DBClient().newDocument().set("encode", arr.map(a => a.asInstanceOf[AnyRef]).asJava)
    val  buff = RowcolCodec.encode(newdoc)
    buff.order(ByteOrder.LITTLE_ENDIAN)
    objectOutput.writeInt(buff.capacity())
    objectOutput.write(buff.array(),0,buff.capacity())
  }

  override def readExternal(objectinput: ObjectInput) : Unit = {
    val buffersize = objectinput.readInt()
    val buffer = ByteBufs.allocate(buffersize)
    MapRDBUtils.readBytes(buffer,buffersize,objectinput)
    val doc = RowcolCodec.decode(buffer)
    this.arr = doc.getList("encode").asScala.map(a => a.asInstanceOf[T])
  }

  override def toString = {
    this.arr.toString()
  }

  override def hashCode() : Int = {
    this.arr.size
  }

  override def equals(other: Any) : Boolean = {
    if (other.isInstanceOf[DBArrayValue[_]]) {
      val that = other.asInstanceOf[DBArrayValue[_]]
      val result = this.sameElements(that)
      return result
    } else if (other.isInstanceOf[Seq[_]]) {
      val that = new DBArrayValue(other.asInstanceOf[Seq[_]])
      val result = this.arr.sameElements(that)
      return result
    }
    return false
  }
}
