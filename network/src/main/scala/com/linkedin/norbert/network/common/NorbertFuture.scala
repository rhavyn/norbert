/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert
package network
package common

import java.util.concurrent._
import atomic.AtomicInteger
import logging.Logging
import annotation.tailrec

class ResponseQueue[ResponseMsg] extends java.util.concurrent.LinkedBlockingQueue[Either[Throwable, ResponseMsg]] {
  def += (res: Either[Throwable, ResponseMsg]): ResponseQueue[ResponseMsg] = {
    add(res)
    this
  }
}

class FutureAdapter[ResponseMsg] extends Future[ResponseMsg] with Function1[Either[Throwable, ResponseMsg], Unit] with ResponseHelper {
  private val latch = new CountDownLatch(1)
  @volatile private var response: Either[Throwable, ResponseMsg] = null

  override def apply(callback: Either[Throwable, ResponseMsg]): Unit = {
    response = callback
    latch.countDown
  }

  def cancel(mayInterruptIfRunning: Boolean) = false

  def isCancelled = false

  def isDone = latch.getCount == 0

  def get = {
    latch.await
    translateResponse(response)
  }

  def get(timeout: Long, timeUnit: TimeUnit) =
    if (latch.await(timeout, timeUnit)) translateResponse(response) else throw new TimeoutException
}

class NorbertResponseIterator[ResponseMsg](numResponses: Int, queue: ResponseQueue[ResponseMsg]) extends ResponseIterator[ResponseMsg] with ResponseHelper {
  private val remaining = new AtomicInteger(numResponses)

  def next = {
    remaining.decrementAndGet
    translateResponse(queue.take)
  }

  def next(timeout: Long, unit: TimeUnit) = queue.poll(timeout, unit) match {
    case null =>
      remaining.decrementAndGet
      throw new TimeoutException("Timed out waiting for response")

    case e =>
      remaining.decrementAndGet
      translateResponse(e)
  }

  def nextAvailable = queue.size > 0

  def hasNext = remaining.get > 0
}

/**
 * An iterator that will timeout after a set amount of time spent waiting on remote data
 */
case class TimeoutIterator[ResponseMsg](inner: ResponseIterator[ResponseMsg], timeout: Long = 5000L) extends ResponseIterator[ResponseMsg] {
  private val timeLeft = new AtomicInteger(timeout.asInstanceOf[Int])

  def hasNext = inner.hasNext && timeLeft.get() > 0

  def nextAvailable = inner.nextAvailable

  def next: ResponseMsg = {
    val before = System.currentTimeMillis
    val res = inner.next(timeLeft.get, TimeUnit.MILLISECONDS)
    val time = (System.currentTimeMillis - before).asInstanceOf[Int]

    timeLeft.addAndGet(-time)
    res
  }

  def next(t: Long, unit: TimeUnit): ResponseMsg = {
    val before = System.currentTimeMillis
    val methodTimeout = unit.toMillis(t)
    val res = inner.next(math.min(methodTimeout, timeLeft.get), TimeUnit.MILLISECONDS)
    val time = (System.currentTimeMillis - before).asInstanceOf[Int]

    timeLeft.addAndGet(-time)
    res
  }
}

/**
 * An optional iterator you can use that exposes the success/failure options for each message
 * as a monad
 */
case class ExceptionIterator[ResponseMsg](inner: ResponseIterator[ResponseMsg]) extends ResponseIterator[Either[Exception, ResponseMsg]] {
  def hasNext = inner.hasNext

  def nextAvailable = inner.nextAvailable

  def next = try {
    val result = inner.next
    Right(result)
  } catch {
    case ex: Exception =>
    Left(ex)
  }

  def next(timeout: Long, unit: TimeUnit) = try {
    val result = inner.next(timeout, unit)
    Right(result)
  } catch {
    case ex: Exception =>
    Left(ex)
  }
}

/**
 * A "partial iterator" If there's an exception during one of the computations, the iterator will simply ignore
 * that exception
 * This is useful for scatter-gather algorithms that may be able to temporarily tolerate partial results for
 * stability, such as in the case of search.
 */
case class PartialIterator[ResponseMsg](inner: ExceptionIterator[ResponseMsg]) extends ResponseIterator[ResponseMsg] {
  var nextElem: Either[Exception, ResponseMsg] = null

  def hasNext: Boolean = hasNext0

  @tailrec private final def hasNext0: Boolean = {
    if(nextElem != null) {
      if (nextElem.isRight) true
      else {
        nextElem = null
        hasNext0
      }
    } else {
      if (inner.hasNext) {
        nextElem = inner.next
        hasNext0
      } else {
        false
      }
    }
  }

  def nextAvailable = inner.nextAvailable

  def next = {
    val hn = hasNext
    if(hn) {
      val result = nextElem.right.get
      nextElem = null
      result
    } else {
      throw new NoSuchElementException()
    }
  }

  def next(timeout: Long, unit: TimeUnit) = {
    next // ignore the timeout since we already must "prime the pump" by calling hasNext. You really should use a timeout iterator underneath the exception iterator.
  }
}

private[common] trait ResponseHelper extends Logging {
  protected def translateResponse[T](response: Either[Throwable, T]) = {
    val r = if(response == null) Left(new NullPointerException("Null response found"))
            else response

    r.fold(ex => throw new ExecutionException(ex) , msg => msg)
 }
}