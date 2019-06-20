package com.mapr.db.spark.sql.concurrent

import scala.concurrent.ExecutionContext

private[concurrent] object UnboundedConcurrentContext extends ConcurrentContext {
  /**
    * We allow implementation to define the ExecutionContext to be used.
    *
    * @return ExecutionContext to be used when spawning new threads.
    */
  override def ec: ExecutionContext = scala.concurrent.ExecutionContext.global
}
