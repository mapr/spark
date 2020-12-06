package org.apache.spark.streaming.kafka.producer.sql

import org.apache.spark.sql.connector.write.WriterCommitMessage

private case class CommittedIds(partitionId: Int, ids: Set[String]) extends WriterCommitMessage