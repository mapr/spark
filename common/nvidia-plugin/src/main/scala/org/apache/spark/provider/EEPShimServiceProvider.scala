/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.provider

import com.nvidia.spark.rapids.SparkShimVersion
import org.apache.spark.SPARK_VERSION_SHORT

object EEPShimServiceProvider {
  val Array(major, minor, patch) = SPARK_VERSION_SHORT.split("\\.").map(_.toInt)
  val VERSION = SparkShimVersion(major, minor, patch)
  val VERSIONNAMES = Seq(s"$VERSION")
}

class EEPShimServiceProvider extends com.nvidia.spark.rapids.SparkShimServiceProvider {

  override def getShimVersion: SparkShimVersion = EEPShimServiceProvider.VERSION

  override def matchesVersion(version: String): Boolean = {
    EEPShimServiceProvider.VERSIONNAMES.contains(version)
  }
}
