/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.exceptions

import com.mapr.db.exceptions.DBException

class SchemaMappingException(message: String, throwable: Throwable)
    extends DBException(message, throwable) {

  def this(message: String) = {
    this(message, null)
  }
}
