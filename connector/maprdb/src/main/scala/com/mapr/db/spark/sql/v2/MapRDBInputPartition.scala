package com.mapr.db.spark.sql.v2

import org.apache.spark.sql.connector.read.InputPartition

case class MapRDBInputPartition(internalId: Int, locations: Array[String], queryJson: String) extends InputPartition
