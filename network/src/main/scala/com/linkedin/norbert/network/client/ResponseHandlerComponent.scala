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
package client

import protos.NorbertProtos
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit, ThreadPoolExecutor, Executors}
import protos.NorbertProtos.NorbertMessage
import logging.Logging
import util.NamedPoolThreadFactory

trait ResponseHandlerComponent {
  val responseHandler: ResponseHandler
}

trait ResponseHandler {
  def onSuccess[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg], message: NorbertProtos.NorbertMessage)
  def onFailure[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg], error: Throwable)
  def shutdown: Unit
}

class ThreadPoolResponseHandler(corePoolSize: Int, maxPoolSize: Int,
    keepAliveTime: Int, maxWaitingQueueSize: Int) extends ResponseHandler with Logging {

  private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS,
    new ArrayBlockingQueue[Runnable](maxWaitingQueueSize), new NamedPoolThreadFactory("norbert-response-handler"))

  def shutdown {
    threadPool.shutdown
    log.debug("Thread pool response handler shut down")
  }

  def onSuccess[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg], message: NorbertMessage) {
    threadPool.execute(new Runnable {
      def run = {
        try {
          val data = message.getMessage.toByteArray
          request.onSuccess(data)
        } catch {
          case ex: Exception =>
            request.onFailure(ex)
        }
      }
    })
  }

  def onFailure[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg], error: Throwable) {
    threadPool.execute(new Runnable {
      def run = request.onFailure(error)
    })
  }
}