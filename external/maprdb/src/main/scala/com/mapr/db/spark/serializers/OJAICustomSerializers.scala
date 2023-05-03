/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.serializers

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.mapr.db.spark.types.DBBinaryValue
import com.mapr.db.util.ByteBufs
import org.ojai.types.{ODate, OInterval, OTime, OTimestamp}



class ODateSerializer extends com.esotericsoftware.kryo.Serializer[ODate]{
  override def write(kryo: Kryo, output: Output, a: ODate): Unit = {
    output.writeInt(a.toDaysSinceEpoch)
  }
  override def read(kryo: Kryo, input: Input, t: Class[ODate]): ODate = {
    val date = ODate.fromDaysSinceEpoch(input.readInt())
    date
  }
}

class OTimeSerializer extends com.esotericsoftware.kryo.Serializer[OTime]{
  override def write(kryo: Kryo, output: Output, a: OTime): Unit = {
    output.writeInt(a.toTimeInMillis)
  }
  override def read(kryo: Kryo, input: Input, t: Class[OTime]): OTime = {
    val time = OTime.fromMillisOfDay(input.readInt())
    time
  }
}

class OTimeStampSerializer extends com.esotericsoftware.kryo.Serializer[OTimestamp]{
  override def write(kryo: Kryo, output: Output, a: OTimestamp): Unit = {
    output.writeLong(a.getMillis)
  }
  override def read(kryo: Kryo, input: Input, t: Class[OTimestamp]): OTimestamp = {
    val timestmp = new OTimestamp(input.readLong())
    timestmp
  }
}

class OIntervalSerializer extends com.esotericsoftware.kryo.Serializer[OInterval]{
  override def write(kryo: Kryo, output: Output, a: OInterval): Unit = {
    output.writeLong(a.getMilliseconds)
  }
  override def read(kryo: Kryo, input: Input, t: Class[OInterval]): OInterval = {
    val milis = new OInterval(input.readLong())
    milis
  }
}

class DBBinaryValueSerializer extends com.esotericsoftware.kryo.Serializer[DBBinaryValue]{
  override def write(kryo: Kryo, output: Output, a: DBBinaryValue): Unit = {
    output.writeInt(a.getByteBuffer().capacity())
    output.write(a.getByteBuffer().array())
  }
  override def read(kryo: Kryo, input: Input, t: Class[DBBinaryValue]): DBBinaryValue = {
    val length = input.readInt()
    val bytearray = new Array[Byte](length)
    input.read(bytearray)
    new DBBinaryValue(ByteBufs.wrap(bytearray))
  }
}

class DBByteBufferValueSerializer extends com.esotericsoftware.kryo.Serializer[ByteBuffer]{
  override def write(kryo: Kryo, output: Output, a: ByteBuffer): Unit = {
    output.writeInt(a.capacity())
    output.write(a.array())
  }
  override def read(kryo: Kryo, input: Input, t: Class[ByteBuffer]): ByteBuffer = {
    val length = input.readInt()
    val bytearray = new Array[Byte](length)
    input.read(bytearray)
    ByteBufs.wrap(bytearray)
  }
}

