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
package com.linkedin.norbert.network.common

import com.google.protobuf.Message
import java.util.concurrent._
import com.linkedin.norbert.network.ResponseIterator
import atomic.AtomicInteger

class NorbertFuture extends Future[Message] with ResponseHelper {
  private val latch = new CountDownLatch(1)
  @volatile private var response: Either[Throwable, Message] = null

  def get(timeout: Long, unit: TimeUnit): Message = if (latch.await(timeout, unit)) translateResponse(response) else throw new TimeoutException

  def get: Message = {
    latch.await
    translateResponse(response)
  }

  def isDone: Boolean = latch.getCount == 0

  def isCancelled: Boolean = false

  def cancel(mayInterruptIfRunning: Boolean): Boolean = false

  def offerResponse(r: Either[Throwable, Message]) {
    response = r
    latch.countDown
  }
}

class NorbertResponseIterator(numResponses: Int) extends ResponseIterator with ResponseHelper {
  private val remaining = new AtomicInteger(numResponses)
  private val responses = new LinkedBlockingQueue[Either[Throwable, Message]]

  def next = {
    remaining.decrementAndGet
    translateResponse(responses.take)
  }

  def next(timeout: Long, unit: TimeUnit) = responses.poll(timeout, unit) match {
    case null => throw new TimeoutException("Timed out waiting for response")

    case e =>
      remaining.decrementAndGet
      translateResponse(e)
  }

  def nextAvailable = responses.size > 0

  def hasNext = remaining.get > 0

  def offerResponse(response: Either[Throwable, Message]) = responses.offer(response)
}

private[common] trait ResponseHelper {
  protected def translateResponse(response: Either[Throwable, Message]) = response.fold(ex => throw new ExecutionException(ex), msg => msg)
}
