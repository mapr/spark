/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark

import com.mapr.db.spark.writers.OJAIValue

import org.apache.spark.streaming.dstream.DStream

package object streaming {

  implicit def toDStreamFunctions[T: OJAIValue](ds: DStream[T]): DStreamFunctions[T] =
    new DStreamFunctions[T](ds)
}
