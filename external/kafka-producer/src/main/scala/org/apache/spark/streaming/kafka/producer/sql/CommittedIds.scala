package org.apache.spark.streaming.kafka.producer.sql

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage

private case class CommittedIds(partitionId: Int, ids: Set[String]) extends WriterCommitMessage