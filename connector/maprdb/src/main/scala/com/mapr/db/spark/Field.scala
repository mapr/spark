/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark

import com.mapr.db.spark.condition._
import com.mapr.db.spark.utils.MapRDBUtils
import org.ojai.store.QueryCondition

/**
* field class provides the functionality to represent the query conditions.
* @param fieldPath name of the field in MapRDB Table.
* @example An equality condition can be represented by
*          field("a.c.d") === 10
*          Similarly a greater than condition can be represented by
*          field("a.c.d") >= 10
*/
  // scalastyle:off class.name
  // scalastyle:off object.name
case class field(fieldPath: String) {

  /**
    * Function to provide greaterThan(>) functionality for a field.
    * @param rhs right hand side value of type T
    * @example field("a.d.d") > 10
    */
  def >[T](rhs: T)(implicit ev: quotes[T]): Predicate = GreaterThan[T](this, rhs)

  /**
    * Function to provide greaterThan EqualsTo(>=) functionality for a field.
    * @param rhs right hand side value of type T
    * @example field("a.d.d") >= 10
    */
  def >=[T](rhs: T)(implicit ev: quotes[T]): Predicate = GreaterThanEquals(this, rhs)

  /**
    * Function to provide lessThan(<) functionality for a field.
    * @param rhs right hand side value of type T
    * @example field("a.d.d") < 10
    */
  def <[T](rhs: T)(implicit ev: quotes[T]): Predicate = LessThan(this, rhs)

  /**
    * Function to provide lessThan EqualsTo(<=) functionality for a field.
    * @param rhs right hand side value of type T
    * @example field("a.d.d") <= 10
    */
  def <=[T](rhs: T)(implicit ev: quotes[T]): Predicate = LessThanEquals(this, rhs)

  /**
    * Function to provide equalsTo(===) functionality for a field.
    * @param rhs right hand side value of type T
    * @example field("a.d.d") === 10
    */
  def ===[T](rhs: T)(implicit ev: quotes[T]): Predicate = EqualsTo(this, rhs)

  /**
    * Function to provide equalsTo(===) functionality for a field to a Sequence.
    * @param rhs right hand side value of type Sequence
    * @example field("a.d.d") === Seq("aa",10)
    */
  def ===(rhs: Seq[Any]): Predicate = EqualsToSeq(this, MapRDBUtils.convertToSeq(rhs))

  /**
    * Function to provide equalsTo(===) functionality for a field to a Map
    * @param rhs right hand side value of type Map[String, Any]
    * @example field("a.d.d") === Map("aa" -> value)
    */
  def ===(rhs: Map[String, Any]): Predicate = EqualsToMap(this, MapRDBUtils.convertToMap(rhs))

  /**
    * Function to provide notEqualsTo(!=) functionality for a field.
    * @param rhs right hand side value of type T
    * @example field("a.d.d") != 10
    */
  def !=[T](rhs: T)(implicit ev: quotes[T]): Predicate = NotEqualsTo(this, rhs)

  /**
    * Function to provide notequalsTo(!=) functionality for a field to a Sequence.
    * @param rhs right hand side value of type Sequence
    * @example field("a.d.d") != Seq("aa",10)
    */
  def !=(rhs: Seq[Any]): Predicate = NotEqualsToSeq(this, MapRDBUtils.convertToSeq(rhs))

  /**
    * Function to provide notequalsTo(!=) functionality for a field to a Map
    * @param rhs right hand side value of type Map[String, Any]
    * @example field("a.d.d") != Map("aa" -> value)
    */
  def !=(rhs: Map[String, Any]): Predicate = NotEqualsToMap(this, MapRDBUtils.convertToMap(rhs))

  /**
    * Function to provide between functionality for a field.
    * @param rhs1 first right hand side value of type T
    * @param rhs2 second right hand side value of type T
    * @example field("a.d.d") between (10,20)
    */
  def between[T](rhs1: T, rhs2: T)(implicit ev: quotes[T]): Predicate = Between(this, rhs1, rhs2)

  /**
    * Function to provide EXISTS functionality for a field.
    * @example field("a.d.d") exists
    */
  def exists(): Exists = Exists(this)

  /**
    * Function to provide NOTIN functionality for a field.
    * @param rhs right hand side value of type Seq[Any]
    * @example field("a.d.d") notin Seq(10,20)
    */
  def notin(rhs: Seq[Any]): NotIn = NotIn(this, MapRDBUtils.convertToSeq(rhs))

  /**
    * Function to provide IN functionality for a field.
    * @param rhs right hand side value of type Seq[Any]
    * @example field("a.d.d") in (10, 20)
    */
  def in(rhs: Seq[Any]): In = In(this, MapRDBUtils.convertToSeq(rhs))

  /**
    * Function to provide NOTEXISTS functionality for a field.
    * @example field("a.d.d") notexists
    */
  def notexists(): NotExists = NotExists(this)

  /**
    * Function to provide TYPEOF functionality for a field.
    * @param typevalue type of the field.
    * @example field("a.d.d") typeof "INT"
    */
  def typeof(typevalue: String): TypeOf = TypeOf(this, typevalue)

  /**
    * Function to provide NOTTYPEOF functionality for a field.
    * @param typevalue type of the field
    * @example field("a.d.d") NOTTYPEOF "INT"
    */
  def nottypeof(typevalue: String): NotTypeOf = NotTypeOf(this, typevalue)

  /**
    * Function to provide LIKE functionality for a field.
    * @param regex right hand side is a SQL like regex string
    * @example field("a.d.d") like "%s"
    */
  def like(regex: String): Like = Like(this, regex)

  /**
    * Function to provide NOTLIKE functionality for a field.
    * @param regex right hand side is a SQL like regex string
    * @example field("a.d.d") notlike "%s"
    */
  def notlike(regex: String): NotLike = NotLike(this, regex)

  /**
    * Function to provide MATCHES functionality for a field.
    * @param regex right hand side is a regular expression
    * @example field("a.d.d") matches "*s"
    */
  def matches(regex: String): Matches = Matches(this, regex)

  /**
    * Function to provide NOTMATCHES functionality for a field.
    * @param regex right hand side is a regular expression
    * @example field("a.d.d") notmatches "*s"
    */
  def notmatches(regex: String): NotMatches = NotMatches(this, regex)

  override def toString: String = s"\42 $fieldPath \42"
}

object field {
  val typemap = Map(
    "INT" -> org.ojai.Value.Type.INT,
    "INTEGER" -> org.ojai.Value.Type.INT,
    "LONG" -> org.ojai.Value.Type.LONG,
    "BOOLEAN" -> org.ojai.Value.Type.BOOLEAN,
    "STRING" -> org.ojai.Value.Type.STRING,
    "SHORT" -> org.ojai.Value.Type.SHORT,
    "BYTE" -> org.ojai.Value.Type.BYTE,
    "NULL" -> org.ojai.Value.Type.NULL,
    "FLOAT" -> org.ojai.Value.Type.FLOAT,
    "DOUBLE" -> org.ojai.Value.Type.DOUBLE,
    "DECIMAL" -> org.ojai.Value.Type.DECIMAL,
    "DATE" -> org.ojai.Value.Type.DATE,
    "TIME" -> org.ojai.Value.Type.TIME,
    "TIMESTAMP" -> org.ojai.Value.Type.TIMESTAMP,
    "INTERVAL" -> org.ojai.Value.Type.INTERVAL,
    "BINARY" -> org.ojai.Value.Type.BINARY,
    "MAP" -> org.ojai.Value.Type.MAP,
    "ARRAY" -> org.ojai.Value.Type.ARRAY
  )
}

case class sizeOf(field: field) {

  /**
    * Function to provide sizeOf lessThan functionality for a field.
    * @param size right hand side is size in long
    * @example sizeOf(field("a.d.d")) < 10
    */
  def <(size: Long): SizeOf = SizeOf(field, QueryCondition.Op.LESS, size)

  /**
    * Function to provide sizeOf greaterThan functionality for a field.
    * @param size right hand side is size in long
    * @example sizeOf(field("a.d.d")) > 10
    */
  def >(size: Long): SizeOf = SizeOf(field, QueryCondition.Op.GREATER, size)

  /**
    * Function to provide sizeOf greaterThan equals to functionality for a field
    * @param size right hand side is size in long
    * @example sizeOf(field("a.d.d")) >= 10
    */
  def >=(size: Long): SizeOf = SizeOf(field, QueryCondition.Op.GREATER_OR_EQUAL, size)

  /**
    * Function to provide sizeOf lessThan equals to functionality for a field
    * @param size right hand side is size in long
    * @example sizeOf(field("a.d.d")) <= 10
    */
  def <=(size: Long): SizeOf = SizeOf(field, QueryCondition.Op.LESS_OR_EQUAL, size)

  /**
    * Function to provide sizeOf equals to functionality for a field.
    * @param size right hand side is a size in long
    * @example sizeOf(field("a.d.d")) === 10
    */
  def ===(size: Long): SizeOf = SizeOf(field, QueryCondition.Op.EQUAL, size)

  /**
    * Function to provide sizeOf not equals to functionality for a field.
    * @param size right hand side is a size in long
    * @example sizeOf(field("a.d.d")) != 10
    */
  def !=(size: Long): SizeOf = SizeOf(field, QueryCondition.Op.NOT_EQUAL, size)
}