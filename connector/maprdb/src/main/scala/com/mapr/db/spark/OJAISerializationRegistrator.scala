/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers._
import com.mapr.db.spark.configuration.SerializableConfiguration
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.serializers._
import com.mapr.db.spark.types.{DBArrayValue, DBBinaryValue, DBMapValue}
import org.ojai.types.{ODate, OInterval, OTime, OTimestamp}

import org.apache.spark.serializer.KryoRegistrator

/**
* Custom registrator provided for registering classes specific to spark ojai connector
* This registrator should be used when kryo serialization is enabled for the spark application.
*
* @example sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
*                   .set("spark.kryo.registrator", "com.mapr.db.spark.OJAIKryoRegistrator")
*/
class OJAIKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[ODate], new ODateSerializer())
    kryo.register(classOf[OTime], new OTimeSerializer())
    kryo.register(classOf[OTimestamp], new OTimeStampSerializer())
    kryo.register(classOf[OInterval], new OIntervalSerializer())
    kryo.register(classOf[DBMapValue], new ExternalizableSerializer())
    kryo.register(classOf[DBBinaryValue], new DBBinaryValueSerializer)
    kryo.register(classOf[SerializableConfiguration], new OjaiJavaSerializer())
    kryo.register(classOf[DBArrayValue[_]], new ExternalizableSerializer())
    kryo.register(classOf[OJAIDocument], new ExternalizableSerializer())
  }
}
