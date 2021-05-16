/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.utils

object DefaultClass extends Serializable {

  sealed class DefaultType[A, B] extends Serializable

  object DefaultType extends BaseClassDefaultType {

    implicit def default[B]: DefaultType[B, B] = new DefaultType[B, B]
  }

  trait BaseClassDefaultType {

    implicit def overrideDefault[A, B]: DefaultType[A, B] = new DefaultType[A, B]
  }
}