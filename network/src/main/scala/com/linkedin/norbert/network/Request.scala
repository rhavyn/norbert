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
package com.linkedin.norbert.network

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import java.util.UUID
import com.google.protobuf.Message

object Request {
  def apply(message: Message, numResponses: Int): Request = Request(message, numResponses, new Request.ResponseIteratorImpl(numResponses))
  
  class ResponseIteratorImpl(numResponses: Int) extends ResponseIterator {
    private val remaining = new AtomicInteger(numResponses)
    private val responses = new LinkedBlockingQueue[Either[Throwable, Message]]

    def hasNext = remaining.get > 0

//    def next = next(responses.poll)

    def next = null

    def nextAvailable = false

//    def next(timeout: Long, unit: TimeUnit) = next(responses.poll(timeout, unit))
    def next(timeout: Long, unit: TimeUnit) = null

    def offerResponse(response: Either[Throwable, Message]) = responses.offer(response)

    def next(block: => Either[Throwable, Message]) = block match {
      case null => None
      case r =>
        remaining.decrementAndGet
        Some(r)
    }
  }
}

case class Request(message: Message, numResponses: Int, responseIterator: Request.ResponseIteratorImpl) {
  val id = UUID.randomUUID
  val timestamp = System.currentTimeMillis

  def offerResponse(response: Either[Throwable, Message]): Unit = responseIterator.offerResponse(response)
}
