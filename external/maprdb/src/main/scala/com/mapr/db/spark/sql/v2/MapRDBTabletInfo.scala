package com.mapr.db.spark.sql.v2

/**
  * Information about the MapR-DB tablet needed by each reader
  *
  * @param internalId Internal tablet identifier
  * @param locations  Preferred location where this task is executed by Spark in order to maintain data locality
  * @param queryJson  Extra query to better perform the filtering based on the data for this tablet / region (JSON FORMAT)
  */
case class MapRDBTabletInfo private[sql](internalId: Int, locations: Array[String], queryJson: String)
