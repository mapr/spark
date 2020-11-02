/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.db.spark.codec

import java.io.IOException

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonProcessingException, Version}
import com.fasterxml.jackson.core.util.VersionUtil
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.ojai.annotation.API
import org.ojai.beans.jackson.JacksonHelper
import org.ojai.types.{ODate, OInterval, OTime, OTimestamp}

@API.Internal
object JacksonBeanCodecHelper {

  val MAPPER: ObjectMapper = new ObjectMapper with ScalaObjectMapper

  class ByteSerializer extends JsonSerializer[Byte] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def serialize(value: Byte, jgen: JsonGenerator, provider: SerializerProvider) {
      jgen.writeObject(value)
    }
  }

  class IntervalSerializer extends JsonSerializer[OInterval] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def serialize(value: OInterval, jgen: JsonGenerator, provider: SerializerProvider) {
      jgen.writeObject(value)
    }
  }

  class IntervalDeserializer extends JsonDeserializer[OInterval] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(p: JsonParser, ctxt: DeserializationContext): OInterval = {
      p.getEmbeddedObject.asInstanceOf[OInterval]
    }
  }

  class DateSerializer extends JsonSerializer[ODate] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def serialize(value: ODate, jgen: JsonGenerator, provider: SerializerProvider) {
      jgen.writeObject(value)
    }
  }

  class DateDeserializer extends JsonDeserializer[ODate] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(p: JsonParser, ctxt: DeserializationContext): ODate = {
      p.getEmbeddedObject.asInstanceOf[ODate]
    }
  }

  class TimeSerializer extends JsonSerializer[OTime] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def serialize(value: OTime, jgen: JsonGenerator, provider: SerializerProvider) {
      jgen.writeObject(value)
    }
  }

  class TimeDeserializer extends JsonDeserializer[OTime] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(p: JsonParser, ctxt: DeserializationContext): OTime = {
      p.getEmbeddedObject.asInstanceOf[OTime]
    }
  }

  class TimestampSerializer extends JsonSerializer[OTimestamp] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def serialize(value: OTimestamp, jgen: JsonGenerator, provider: SerializerProvider) {
      jgen.writeObject(value)
    }
  }

  class TimestampDeserializer extends JsonDeserializer[OTimestamp] {
    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(p: JsonParser, ctxt: DeserializationContext): OTimestamp = {
      p.getEmbeddedObject.asInstanceOf[OTimestamp]
    }
  }

  val version: Version = {
    val ver: String = classOf[JacksonHelper].getPackage.getImplementationVersion
    if (ver == null) {
      VersionUtil.parseVersion("<unknown>", "org.ojai", "core")
    } else {
      VersionUtil.parseVersion(ver, "org.ojai", "core")
    }
  }

  val byteSerializer: JacksonBeanCodecHelper.ByteSerializer =
    new JacksonBeanCodecHelper.ByteSerializer

  val module: SimpleModule = new SimpleModule("OjaiSerializers", version)
  module.addSerializer(classOf[Byte], byteSerializer)
  module.addSerializer(classOf[OInterval], new JacksonHelper.IntervalSerializer)
  module.addDeserializer(classOf[OInterval], new JacksonHelper.IntervalDeserializer)
  module.addSerializer(classOf[ODate], new JacksonHelper.DateSerializer)
  module.addDeserializer(classOf[ODate], new JacksonHelper.DateDeserializer)
  module.addSerializer(classOf[OTime], new JacksonHelper.TimeSerializer)
  module.addDeserializer(classOf[OTime], new JacksonHelper.TimeDeserializer)
  module.addSerializer(classOf[OTimestamp], new JacksonHelper.TimestampSerializer)
  module.addDeserializer(classOf[OTimestamp], new JacksonHelper.TimestampDeserializer)
  MAPPER.registerModule(DefaultScalaModule)
  MAPPER.registerModule(module)

}

