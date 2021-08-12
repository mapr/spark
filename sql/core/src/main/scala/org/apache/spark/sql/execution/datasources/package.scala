package org.apache.spark.sql.execution

import org.apache.spark.SparkEnv

package object datasources {
  val checkForSymlinkConfKey: String = "spark.mapr.symlinkSupport"
  val checkForSymlink = SparkEnv.get.conf.getBoolean(checkForSymlinkConfKey, true)
}

