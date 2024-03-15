/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.types

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.{ByteBuffer, ByteOrder}

import com.mapr.db.impl.IdCodec
import com.mapr.db.spark.utils.MapRDBUtils
import com.mapr.db.util.ByteBufs
import com.mapr.org.apache.hadoop.hbase.util.Bytes

private[spark] final class DBBinaryValue(
    @transient private[spark] var bin: ByteBuffer)
    extends Externalizable {
  def this() = {
    this(null)
  }

  override def writeExternal(objectOutput: ObjectOutput): Unit = {
    objectOutput.writeInt(bin.capacity())
    bin.order(ByteOrder.LITTLE_ENDIAN)
    objectOutput.write(bin.array())
  }

  override def readExternal(objectinput: ObjectInput): Unit = {
    val buffersize = objectinput.readInt()
    val buffer = ByteBufs.allocate(buffersize)
    MapRDBUtils.readBytes(buffer, buffersize, objectinput)
    this.bin = buffer
  }

  def getByteBuffer(): ByteBuffer = this.bin

  def array(): Array[Byte] = this.bin.array()

  private def getval = this.bin

  override def toString: String = this.bin.toString

  override def hashCode(): Int = this.bin.array().length

  override def equals(other: Any): Boolean = {
    if (!other.isInstanceOf[DBBinaryValue]) {
      false
    } else {
      val prevBuf = IdCodec.encodeAsBytes(this.bin)
      val curBuf = IdCodec.encodeAsBytes(other.asInstanceOf[DBBinaryValue].bin)
      Bytes.compareTo(prevBuf, 0, prevBuf.length, curBuf, 0, curBuf.length) == 0
    }
  }
}
