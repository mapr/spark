package com.mapr.db.spark.sql.ojai

sealed trait JoinType

object JoinType {

  def apply(value: String): JoinType = joins.indexWhere(_.toString == value.toLowerCase()) match {
    case -1 => throw new IllegalArgumentException(s"$value is not a supported join type")
    case idx => joins(idx)
  }

  private lazy val joins = List(inner, outer, full, left, left_outer)

  case object inner extends JoinType {
    override def toString: String = "inner"
  }

  case object outer extends JoinType {
    override def toString: String = "outer"
  }

  case object full extends JoinType {
    override def toString: String = "full"
  }

  case object left extends JoinType {
    override def toString: String = "left"
  }

  case object left_outer extends JoinType {
    override def toString: String = "left_outer"
  }

}