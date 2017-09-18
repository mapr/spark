/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.codec

import org.ojai.beans.jackson.JacksonHelper
import java.io.IOException
import org.ojai.annotation.API
import org.ojai.types.ODate
import org.ojai.types.OInterval
import org.ojai.types.OTime
import org.ojai.types.OTimestamp
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.core.util.VersionUtil
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

@API.Internal
object JacksonBeanCodecHelper {

  var VERSION: Version = null
  val MAPPER: ObjectMapper = new ObjectMapper

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
      return p.getEmbeddedObject.asInstanceOf[OInterval]
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
      return p.getEmbeddedObject.asInstanceOf[ODate]
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
      return p.getEmbeddedObject.asInstanceOf[OTime]
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
      return p.getEmbeddedObject.asInstanceOf[OTimestamp]
    }
  }

  val version: String = classOf[JacksonHelper].getPackage.getImplementationVersion
  val version_str: String = if (version == null) "<unknown>"
  else version
  VERSION = VersionUtil.parseVersion(version_str, "org.ojai", "core")
  val module: SimpleModule = new SimpleModule("OjaiSerializers", VERSION)
  val byteSerializer: JacksonBeanCodecHelper.ByteSerializer = new JacksonBeanCodecHelper.ByteSerializer
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

