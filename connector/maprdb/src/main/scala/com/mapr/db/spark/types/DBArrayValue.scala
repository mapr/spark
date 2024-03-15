/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.types

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio._
import scala.collection.{IterableFactoryDefaults, SeqFactory, SeqLike, mutable}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import com.mapr.db.rowcol.RowcolCodec
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.utils.MapRDBUtils
import com.mapr.db.util.ByteBufs

import java.util
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.SeqOps

private[spark] object DBArrayValue extends SeqFactory[DBArrayValue] {
  implicit def canBuildFrom[T]: CanBuildFrom[Iterable[T], T, DBArrayValue[T]] =
    new CanBuildFrom[Iterable[T], T, DBArrayValue[T]] {
      def apply(): mutable.Builder[T, DBArrayValue[T]] =
        DBArrayValue.newBuilder[T]

      override def fromSpecific(from: Iterable[T])(it: IterableOnce[T]): DBArrayValue[T] = {
        val builder = newBuilder(from)
        builder.addAll(it)
        builder.result()
      }

      override def newBuilder(from: Iterable[T]): mutable.Builder[T, DBArrayValue[T]] = DBArrayValue.newBuilder[T]
    }

  def newBuilder[T]: mutable.Builder[T, DBArrayValue[T]] =
    new ListBuffer[T] mapResult (x => new DBArrayValue(x))

  override def from[A](source: IterableOnce[A]): DBArrayValue[A] = new DBArrayValue[A](source.iterator.to(Seq))

  override def empty[A]: DBArrayValue[A] = new DBArrayValue[A](Seq.empty)
}

private[spark] class DBArrayValue[T]( @transient private[spark] var arr : Seq[T])
  extends Seq[T]
    with Externalizable {

  def this() = {
    this(null)
  }

  override def iterableFactory: DBArrayValue.type = DBArrayValue

  def iterator: Iterator[T] = new ListIterator[T](arr)

  def apply(idx: Int): T = {
    if (idx < 0 || idx>=length) throw new IndexOutOfBoundsException
    val element = arr(idx)
    element match {
      case _: util.List[_] =>
        new DBArrayValue(element.asInstanceOf[util.List[Object]].asScala.toSeq).asInstanceOf[T]
      case _: util.Map[_, _] =>
        new DBMapValue(element.asInstanceOf[util.Map[String, Object]].asScala.toMap).asInstanceOf[T]
      case _ => element
    }
  }

  def length: Int = arr.size

  private def getval = this.arr

  override def writeExternal(objectOutput: ObjectOutput): Unit = {
    val newdoc = DBClient().newDocument().set("encode", arr.map(a => a.asInstanceOf[AnyRef]).asJava)
    val  buff = RowcolCodec.encode(newdoc)
    buff.order(ByteOrder.LITTLE_ENDIAN)
    objectOutput.writeInt(buff.capacity())
    objectOutput.write(buff.array(), 0, buff.capacity())
  }

  override def readExternal(objectinput: ObjectInput) : Unit = {
    val buffersize = objectinput.readInt()
    val buffer = ByteBufs.allocate(buffersize)
    MapRDBUtils.readBytes(buffer, buffersize, objectinput)
    val doc = RowcolCodec.decode(buffer)
    this.arr = doc.getList("encode").asScala.toSeq.map(a => a.asInstanceOf[T])
  }

  override def toString: String = this.arr.toString()

  override def hashCode() : Int = {
    this.arr.size
  }

  override def equals(other: Any) : Boolean = {
    other match {
      case that: DBArrayValue[_] =>
        val result = this.sameElements(that)
        return result
      case arr1: Seq[_] =>
        val that = new DBArrayValue(arr1)
        val result = this.arr.sameElements(that)
        return result
      case _ =>
    }

    false
  }
}
