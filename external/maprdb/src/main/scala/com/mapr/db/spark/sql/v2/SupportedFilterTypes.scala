package com.mapr.db.spark.sql.v2

import java.sql.Timestamp

object SupportedFilterTypes {

  private lazy val supportedTypes = List[Class[_]](
    classOf[Double],
    classOf[java.lang.Double],
    classOf[Float],
    classOf[java.lang.Float],
    classOf[Int],
    classOf[Integer],
    classOf[Long],
    classOf[java.lang.Long],
    classOf[Short],
    classOf[java.lang.Short],
    classOf[String],
    classOf[Timestamp],
    classOf[Boolean],
    classOf[java.lang.Boolean],
    classOf[Byte],
    classOf[java.lang.Byte]
  )

  def isSupportedType(value: Any): Boolean = supportedTypes.contains(value.getClass)
}

