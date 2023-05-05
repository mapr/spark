/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.types

import org.apache.spark.SparkFunSuite

class DBMapValueTest extends SparkFunSuite {
  test("Check DBMapValue equals method") {
    val map = Map("1" -> "2", "a" -> "b")
    val dbMapValue = new DBMapValue(map)
    val dbMapValueOther = new DBMapValue(map)

    assert(!dbMapValue.equals("StringType"))
    assert(dbMapValue.equals(map))
    assert(dbMapValue.equals(dbMapValueOther))
  }
}
