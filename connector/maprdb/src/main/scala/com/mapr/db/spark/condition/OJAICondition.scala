/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.db.spark.condition

import scala.collection.JavaConverters._

import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.field
import org.ojai.exceptions.TypeException
import org.ojai.store.QueryCondition


trait Predicate extends Serializable {
  def and(rhs: Predicate): AND = AND(this, rhs)
  def or(rhs: Predicate): OR = OR(this, rhs)
  protected val closeParan = "}"
  protected val openParan = "{"
  protected def prettyPrint(elements: Seq[String]): String =
    elements.mkString(",")
  protected def prettyPrint(mapelems: Map[String, String]): String =
    mapelems
      .foldLeft("") { (s: String, pair: (String, String)) =>
        s + pair._1 + ":" + pair._2 + ","
      }
      .stripSuffix(",")
  def build: QueryCondition
}

trait quotes[T] extends Serializable {
  type Self
  def build(colName: String, value: T, oper: QueryCondition.Op): QueryCondition
}

case class AND(lhs: Predicate, rhs: Predicate) extends Predicate {
  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .and()
      .condition(lhs.build.build())
      .condition(rhs.build.build())
      .close()
}

case class OR(lhs: Predicate, rhs: Predicate) extends Predicate {
  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .or()
      .condition(lhs.build.build())
      .condition(rhs.build.build())
      .close()
}

case class GreaterThan[T: quotes](c: field, rhs: T)
                                 (implicit ev: quotes[T]) extends Predicate {
  override def build: QueryCondition =
    ev.build(c.fieldPath, rhs, QueryCondition.Op.GREATER)
}

case class GreaterThanEquals[T: quotes](c: field, rhs: T)(
    implicit ev: quotes[T])
    extends Predicate {
  override def build: QueryCondition =
    ev.build(c.fieldPath, rhs, QueryCondition.Op.GREATER_OR_EQUAL)
}

case class LessThan[T: quotes](c: field, rhs: T)(implicit ev: quotes[T])
    extends Predicate {
  override def build: QueryCondition =
    ev.build(c.fieldPath, rhs, QueryCondition.Op.LESS)
}

case class LessThanEquals[T: quotes](c: field, rhs: T)(implicit ev: quotes[T])
    extends Predicate {
  override def build: QueryCondition =
    ev.build(c.fieldPath, rhs, QueryCondition.Op.LESS_OR_EQUAL)
}

case class EqualsTo[T: quotes](c: field, rhs: T)(implicit ev: quotes[T])
    extends Predicate {
  override def build: QueryCondition =
    ev.build(c.fieldPath, rhs, QueryCondition.Op.EQUAL)
}

case class EqualsToSeq(c: field, rhs: Seq[AnyRef]) extends Predicate {
  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .equals(c.fieldPath, rhs.asJava)
}

case class EqualsToMap(c: field, rhs: Map[String, AnyRef]) extends Predicate {
  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .equals(c.fieldPath, rhs.asJava)
}

case class NotEqualsTo[T: quotes](c: field, rhs: T)(implicit ev: quotes[T])
    extends Predicate {
  override def build: QueryCondition =
    ev.build(c.fieldPath, rhs, QueryCondition.Op.NOT_EQUAL)
}

case class NotEqualsToSeq(c: field, rhs: Seq[AnyRef]) extends Predicate {
  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .notEquals(c.fieldPath, rhs.asJava)
}

case class NotEqualsToMap(c: field, rhs: Map[String, AnyRef])
    extends Predicate {
  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .notEquals(c.fieldPath, rhs.asJava)
}

case class Between[T: quotes](c: field, rhs1: T, rhs2: T)
                             (implicit ev: quotes[T])
    extends Predicate {

  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .and()
      .condition(ev.build(c.fieldPath, rhs1,
        QueryCondition.Op.GREATER_OR_EQUAL).build())
      .condition(ev.build(c.fieldPath, rhs2,
        QueryCondition.Op.LESS_OR_EQUAL).build())
      .close()
}

case class Exists(c: field) extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().exists(c.fieldPath)
}

case class In(c: field, rhs: Seq[AnyRef]) extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().in(c.fieldPath, rhs.asJava)
}

case class NotIn(c: field, rhs: Seq[AnyRef]) extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().notIn(c.fieldPath, rhs.asJava)
}

case class NotExists(c: field) extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().notExists(c.fieldPath)
}

case class TypeOf(c: field, typevalue: String) extends Predicate {
  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .typeOf(
        c.fieldPath,
        field.typemap
          .getOrElse(typevalue, throw new TypeException("Type: " + typevalue + " doesn't exist")))
}

case class NotTypeOf(c: field, typevalue: String) extends Predicate {
  override def build: QueryCondition =
    DBClient()
      .newCondition()
      .notTypeOf(
        c.fieldPath,
        field.typemap
          .getOrElse(typevalue, throw new TypeException("Type: " + typevalue + " doesn't exist")))
}

case class Like(c: field, regex: String) extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().like(c.fieldPath, regex)
}

case class NotLike(c: field, regex: String) extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().notLike(c.fieldPath, regex)
}

case class Matches(c: field, regex: String) extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().matches(c.fieldPath, regex)
}

case class NotMatches(c: field, regex: String) extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().notMatches(c.fieldPath, regex)
}

case class SizeOf(c: field, op: QueryCondition.Op, size: Long)
    extends Predicate {
  override def build: QueryCondition =
    DBClient().newCondition().sizeOf(c.fieldPath, op, size)
}
