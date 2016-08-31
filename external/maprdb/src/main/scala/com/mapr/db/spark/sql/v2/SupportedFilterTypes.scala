package com.mapr.db.spark.sql.v2

import java.sql.Timestamp

object SupportedFilterTypes {

  private lazy val supportedTypes = List[Class[_]](
    classOf[Double],
    classOf[Float],
    classOf[Int],
    classOf[Long],
    classOf[Short],
    classOf[String],
    classOf[Timestamp],
    classOf[Boolean],
    classOf[Byte]
  )

  def isSupportedType(value: Any): Boolean = supportedTypes.contains(value.getClass)
}

