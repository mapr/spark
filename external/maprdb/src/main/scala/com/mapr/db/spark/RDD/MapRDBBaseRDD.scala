/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import scala.reflect.ClassTag

import com.mapr.db.exceptions.DBException
import com.mapr.db.spark.condition.{DBQueryCondition, Predicate}
import com.mapr.db.spark.dbclient.DBClient
import org.ojai.store.QueryCondition

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

private[spark] abstract class MapRDBBaseRDD[T: ClassTag](
    @transient val sc: SparkContext,
    tableName: String,
    condition: DBQueryCondition,
    beanClass: Class[T],
    fields: Seq[String] = "*" :: Nil,
    queryOptions: Map[String, String] = Map())
    extends RDD[T](sc, Seq.empty) {

  type Self <: MapRDBBaseRDD[T]

  protected def copy(tableName: String = tableName,
                     fields: Seq[String] = fields,
                     cond: DBQueryCondition = condition,
                     beanClass: Class[T] = beanClass,
                     queryOptions: Map[String, String] = queryOptions): Self

  def where(pred: Predicate): Self = {
    if (condition != null && !condition.condition.isEmpty) {
      copy(
        cond = DBQueryCondition(
          DBClient()
            .newCondition()
            .and()
            .condition(condition.condition)
            .condition(pred.build.build())
            .close
            .build()))
    } else {
      copy(cond = DBQueryCondition(pred.build.build()))
    }
  }

  def where(condition: QueryCondition): Self = {
    if (this.condition != null && !this.condition.condition.isEmpty) {
      copy(
        cond = DBQueryCondition(
          DBClient()
            .newCondition()
            .and()
            .condition(this.condition.condition)
            .condition(condition)
            .build()))
    } else {
      copy(cond = DBQueryCondition(condition))
    }
  }

  def limit(value: Integer): Self = {
    throw new NotImplementedError()
  }

  def select[TF: FIELD](projectedFields: TF*)(implicit ev: FIELD[TF]): Self = {
    if (fields == null) {
      copy(fields = ev.getFields(projectedFields))
    } else {
      val fieldProjections = ev.getFields(projectedFields)
      val outputFields = fieldProjections.filter(fld => !this.fields.contains(fld))
      if (outputFields.nonEmpty) {
        throw new DBException(
          "Fields:" + fieldProjections + " doesn't exist in the RDD")
      } else {
        copy(fields = fieldProjections.filter(fld => this.fields.contains(fld)))
      }
    }
  }
}
