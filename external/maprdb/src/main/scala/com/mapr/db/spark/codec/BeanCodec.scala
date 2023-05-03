/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.codec

import java.io.IOException

import com.mapr.db.spark.codec.JacksonBeanCodecHelper._
import org.ojai.{Document, DocumentBuilder, DocumentReader}
import org.ojai.annotation.API
import org.ojai.beans.jackson.DocumentGenerator
import org.ojai.exceptions.{DecodingException, EncodingException}

@API.Internal
object BeanCodec {
  @throws[DecodingException]
  def decode(db: DocumentBuilder, bean: Any): Document = {
    if (bean == null) return null
    val gen: DocumentGenerator = new DocumentGenerator(db)
    try {
      MAPPER.writeValue(gen, bean)
      gen.getDocument
    }
    catch {
      case e: Exception =>
        throw new DecodingException("Failed to convert the java bean to Document", e)
    }
  }

  @throws[EncodingException]
  def encode[T](dr: DocumentReader, beanClass: Class[T]): T = {
    if (dr == null) return null.asInstanceOf[T]
    try {
      MAPPER.readValue(new DocumentParserImpl(dr), beanClass)
    }
    catch {
      case e: IOException =>
        throw new EncodingException("Failed to create java bean from Document", e)
    }
  }
}
