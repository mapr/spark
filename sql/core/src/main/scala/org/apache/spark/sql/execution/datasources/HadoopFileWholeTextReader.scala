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

package org.apache.spark.sql.execution.datasources

import java.io.Closeable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.input.WholeTextFileRecordReader

/**
 * An adaptor from a [[PartitionedFile]] to an [[Iterator]] of [[Text]], which is all of the lines
 * in that file.
 */
class HadoopFileWholeTextReader(file: PartitionedFile, conf: Configuration)
  extends Iterator[Text] with Closeable {
  private val _iterator = {
    val filePathData = FileUtil.checkPathForSymlink(new Path(file.filePath), conf)
    val fileSplit = new CombineFileSplit(
      Array(filePathData.path),
      Array(file.start),
      Array(filePathData.stat.getLen),
      // TODO: Implement Locality
      Array.empty[String])
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
    val reader = new WholeTextFileRecordReader(fileSplit, hadoopAttemptContext, 0)
    reader.setConf(hadoopAttemptContext.getConfiguration)
    reader.initialize(fileSplit, hadoopAttemptContext)
    new RecordReaderIterator(reader)
  }

  override def hasNext: Boolean = _iterator.hasNext

  override def next(): Text = _iterator.next()

  override def close(): Unit = _iterator.close()
}
