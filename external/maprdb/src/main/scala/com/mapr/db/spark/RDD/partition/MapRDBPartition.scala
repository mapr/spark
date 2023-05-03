/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD.partition

import com.mapr.db.spark.condition.DBQueryCondition
import org.apache.spark.Partition
/**
  * An identifier for a partition in a MapRTableScanRDD.
  *
  * @param index The partition's index within its parent RDD
  * @param locations The preferred locations (hostnames) for the data
  * @param tableName name of the table to which this partiton belongs.
  * @param cond queryCondition associated with a partition
  */

case class MaprDBPartition(index: Int, tableName: String, locations: Seq[String],
                           size: Long, cond : DBQueryCondition) extends Partition {

  override def hashCode(): Int = super.hashCode()

  override def equals(other: Any): Boolean = other match {
    case p: MaprDBPartition =>
      tableName.equals(p.tableName) &&  locations.equals(p.locations) && size.equals(p.size)
    case _ => false
  }
}
