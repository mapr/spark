package com.mapr.db.spark.sql.concurrent

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration.Inf

/**
  * TaskLevelConcurrentContext is used to control a multithreaded context within a Spark Task.
  */
trait ConcurrentContext {

  /**
    * Wraps a block within a concurrent tasks
    *
    * @param task Block to be executed in concurrently.
    * @tparam A Result type of the passed in block.
    * @return A concurrent task, that is a Future[A].
    */
  def async[A](task: => A): Future[A] = Future(task)(ec)

  /**
    * Awaits for multiple concurrent tasks using a sliding windows so we don't have to hold all task results in memory
    * at once.
    *
    * @param it        Iterator of concurrent tasks.
    * @param batchSize The number of concurrent tasks we want to wait at a time.
    * @tparam A Result type of each concurrent task.
    * @return An iterator that contains the result of executing each concurrent task.
    */
  def awaitSliding[A](it: Iterator[Future[A]], batchSize: Int = 20): Iterator[A] = {

    implicit val context: ExecutionContext = ec

    val slidingIterator = it.sliding(batchSize - 1).withPartial(true)

    val (head, tail) = slidingIterator.span(_ => slidingIterator.hasNext)

    head.map(batchOfFuture => Await.result(batchOfFuture.head, Inf)) ++
      tail.flatMap(batchOfFuture => Await.result(Future.sequence(batchOfFuture), Inf))
  }

  /**
    * We allow implementations to define the ExecutionContext to be used.
    *
    * @return ExecutionContext to be used when spawning new threads.
    */
  def ec: ExecutionContext
}

object ConcurrentContext {

  /**
    * Implicit instance to our TaskLevelConcurrentContext since it is our default one.
    */
  implicit val defaultConcurrentContext: ConcurrentContext = BoundedConcurrentContext

  def unboundedConcurrentContext: ConcurrentContext = UnboundedConcurrentContext

  /**
    * Implicit syntax
    */
  object Implicits {

    implicit class ConcurrentIteratorOps[A](it: Iterator[Future[A]]) {
      def awaitSliding(batchSize: Int = 20)(implicit concurrentContext: ConcurrentContext): Iterator[A] =
        concurrentContext.awaitSliding(it, batchSize)
    }

    implicit class AsyncOps[A](task: => A) {
      def async(implicit concurrentContext: ConcurrentContext): Future[A] = concurrentContext.async(task)
    }

  }

}