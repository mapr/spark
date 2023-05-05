package com.mapr.db.spark.sql.v2

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.Base64

import org.ojai.store.QueryCondition
import org.ojai.types.OTimestamp
import org.ojai.util.Values

object QueryConditionExtensions {

  /**
    * Generic extensions for OJAI QueryCondition.
    *
    * @param cond QueryCondition to apply operators to.
    */
  implicit class QueryConditionOps(cond: QueryCondition) extends Serializable {

    def equalTo[A](field: String, value: A): QueryCondition = FieldQuery(cond, field) === value

    def notEqual[A](field: String, value: A): QueryCondition = FieldQuery(cond, field) =!= value

    def lessThan[A](field: String, value: A): QueryCondition = FieldQuery(cond, field) < value

    def lessThanEqual[A](field: String, value: A): QueryCondition = FieldQuery(cond, field) <= value

    def greaterThan[A](field: String, value: A): QueryCondition = FieldQuery(cond, field) > value

    def greaterThanEqual[A](field: String, value: A): QueryCondition = FieldQuery(cond, field) >= value

    def ===[A](field: String, value: A): QueryCondition = equalTo(field, value)

    def =!=[A](field: String, value: A): QueryCondition = notEqual(field, value)

    def <[A](field: String, value: A): QueryCondition = lessThan(field, value)

    def <=[A](field: String, value: A): QueryCondition = lessThanEqual(field, value)

    def >[A](field: String, value: A): QueryCondition = greaterThan(field, value)

    def >=[A](field: String, value: A): QueryCondition = greaterThanEqual(field, value)

    def field(field: String): FieldQuery = FieldQuery(cond, field)
  }

  case class FieldQuery(cond: QueryCondition, field: String) {


    def is[A](op: QueryCondition.Op, value: A): QueryCondition = value match {
      case _: Float => cond.is(field, op, value.asInstanceOf[Float])
      case _: BigDecimal => cond.is(field, op, value.asInstanceOf[BigDecimal].bigDecimal)
      case _: Long => cond.is(field, op, value.asInstanceOf[Long])
      case _: Timestamp => cond.is(field, op, new OTimestamp(value.asInstanceOf[Timestamp].getTime))
      case _: Boolean => cond.is(field, op, value.asInstanceOf[Boolean])
      case _: Short => cond.is(field, op, value.asInstanceOf[Short])
      case _: Int => cond.is(field, op, value.asInstanceOf[Int])
      case _id: Byte if field == "_id" => cond.is("$$row_key", op, _id)
      case _: Byte => cond.is(field, op, value.asInstanceOf[Byte])
      case _: Double => cond.is(field, op, value.asInstanceOf[Double])
      case _id: String if field == "_id" => cond.is("$$row_key", op, Values.parseBinary(Base64.getEncoder.encodeToString(("\u0003" + _id).getBytes("UTF-8"))))
      case _: String => cond.is(field, op, value.asInstanceOf[String])
      case _id: ByteBuffer if field == "_id" => cond.is("$$row_key", op, _id)
      case _: ByteBuffer => cond.is(field, op, value.asInstanceOf[ByteBuffer])

      case _ => cond
    }

    def equalTo[A](value: A): QueryCondition = is(QueryCondition.Op.EQUAL, value)

    def notEqual[A](value: A): QueryCondition = is(QueryCondition.Op.NOT_EQUAL, value)

    def lessThan[A](value: A): QueryCondition = is(QueryCondition.Op.LESS, value)

    def lessThanEqual[A](value: A): QueryCondition = is(QueryCondition.Op.LESS_OR_EQUAL, value)

    def greaterThan[A](value: A): QueryCondition = is(QueryCondition.Op.GREATER, value)

    def greaterThanEqual[A](value: A): QueryCondition = is(QueryCondition.Op.GREATER_OR_EQUAL, value)

    def ===[A](value: A): QueryCondition = equalTo(value)

    def =!=[A](value: A): QueryCondition = notEqual(value)

    def <[A](value: A): QueryCondition = lessThan(value)

    def <=[A](value: A): QueryCondition = lessThanEqual(value)

    def >[A](value: A): QueryCondition = greaterThan(value)

    def >=[A](value: A): QueryCondition = greaterThanEqual(value)
  }

}
