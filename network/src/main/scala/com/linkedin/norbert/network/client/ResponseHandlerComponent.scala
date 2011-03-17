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
import norbertutils.NamedPoolThreadFactory
import jmx.JMX.MBean
import server.RequestProcessorMBean
import jmx.JMX

trait ResponseHandlerComponent {
  val responseHandler: ResponseHandler
}

trait ResponseHandler {
  def onSuccess[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg], message: NorbertProtos.NorbertMessage)
  def onFailure[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg], error: Throwable)
  def shutdown: Unit
}

class ThreadPoolResponseHandler(serviceName: String, corePoolSize: Int, maxPoolSize: Int,
    keepAliveTime: Int, maxWaitingQueueSize: Int) extends ResponseHandler with Logging {

  private val responseQueue = new ArrayBlockingQueue[Runnable](maxWaitingQueueSize)

  private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS,
    responseQueue, new NamedPoolThreadFactory("norbert-response-handler"))

  val statsJmx = JMX.register(new ResponseProcessorMBeanImpl(serviceName, responseQueue))

  def shutdown {
    threadPool.shutdown
    statsJmx.foreach(JMX.unregister(_))

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

trait ResponseProcessorMBean {
  def getQueueSize: Int
}

class ResponseProcessorMBeanImpl(serviceName: String, queue: ArrayBlockingQueue[Runnable])
  extends MBean(classOf[RequestProcessorMBean], "service=%s".format(serviceName)) with ResponseProcessorMBean {
  def getQueueSize = queue.size
}
