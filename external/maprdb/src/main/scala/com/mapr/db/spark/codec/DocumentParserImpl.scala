/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.db.spark.codec

import com.fasterxml.jackson.core.JsonLocation
import org.ojai.DocumentReader
import org.ojai.beans.jackson.DocumentParser

class DocumentParserImpl(dr: DocumentReader) extends DocumentParser(dr) {
  override def getCurrentLocation: JsonLocation = {
    JsonLocation.NA
  }
}
