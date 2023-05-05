/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.db.spark

import java.nio.ByteBuffer

import com.mapr.db.spark.dbclient.DBClient
import org.ojai.store.QueryCondition
import org.ojai.types.{ODate, OInterval, OTime, OTimestamp}

package object condition {

  implicit def quotesInteger: quotes[Integer] = new quotes[Integer] {
    override type Self = Integer

    override def build(colName: String, value: Integer, oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesInt: quotes[Int] = new quotes[Int] {
    override type Self = Int

    override def build(colName: String, value: Int, oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesLong: quotes[Long] = new quotes[Long] {
    override type Self = Long

    override def build(colName: String, value: Long, oper: QueryCondition.Op) =
      DBClient()
        .newCondition().
        is(colName, oper, value)
  }

  implicit def quotesString: quotes[String] = new quotes[String] {
    override type Self = String

    override def build(colName: String,
                       value: String,
                       oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesODate: quotes[ODate] = new quotes[ODate] {
    override type Self = ODate

    override def build(colName: String, value: ODate, oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesOTime: quotes[OTime] = new quotes[OTime] {
    override type Self = OTime

    override def build(colName: String, value: OTime, oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesByte: quotes[Byte] = new quotes[Byte] {
    override type Self = Byte

    override def build(colName: String, value: Byte, oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesShort: quotes[Short] = new quotes[Short] {
    override type Self = Short

    override def build(colName: String, value: Short, oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesBoolean: quotes[Boolean] = new quotes[Boolean] {
    override type Self = Boolean

    override def build(colName: String,
                       value: Boolean,
                       oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesBigDecimal: quotes[BigDecimal] = new quotes[BigDecimal] {
    override type Self = BigDecimal

    override def build(colName: String,
                       value: BigDecimal,
                       oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value.bigDecimal)
  }

  implicit def quotesDouble: quotes[Double] = new quotes[Double] {
    override type Self = Double

    override def build(colName: String,
                       value: Double,
                       oper: QueryCondition.Op) =
      DBClient().newCondition().is(colName, oper, value)
  }

  implicit def quotesFloat: quotes[Float] = new quotes[Float] {
    override type Self = Float

    override def build(colName: String, value: Float, oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesOInterval: quotes[OInterval] = new quotes[OInterval] {
    override type Self = OInterval

    override def build(colName: String,
                       value: OInterval,
                       oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesOTimestamp: quotes[OTimestamp] = new quotes[OTimestamp] {
    override type Self = OTimestamp

    override def build(colName: String,
                       value: OTimestamp,
                       oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }

  implicit def quotesByteBuffer: quotes[ByteBuffer] = new quotes[ByteBuffer] {
    override type Self = ByteBuffer

    override def build(colName: String,
                       value: ByteBuffer,
                       oper: QueryCondition.Op) =
      DBClient()
        .newCondition()
        .is(colName, oper, value)
  }
}
