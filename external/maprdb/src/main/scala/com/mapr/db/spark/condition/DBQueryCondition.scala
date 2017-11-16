/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.db.spark.condition

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import com.mapr.db.impl.ConditionImpl
import com.mapr.db.spark.utils.MapRDBUtils
import com.mapr.db.util.ByteBufs
import org.ojai.store.QueryCondition

private[spark] class DBQueryCondition(@transient var condition: QueryCondition)
    extends Externalizable {

  def this() {
    this(null)
  }

  override def readExternal(in: ObjectInput): Unit = {
    val size = in.readInt()
    val condSerialized = ByteBufs.allocate(size)
    MapRDBUtils.readBytes(condSerialized, size, in)
    this.condition = ConditionImpl.parseFrom(condSerialized)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    val desc = condition.asInstanceOf[ConditionImpl].getDescriptor
    val condSerialized = desc.getSerialized
    out.writeInt(condSerialized.capacity())
    out.write(condSerialized.array())
  }
}

object DBQueryCondition {
  def apply(cond: QueryCondition): DBQueryCondition = new DBQueryCondition(cond)
}
