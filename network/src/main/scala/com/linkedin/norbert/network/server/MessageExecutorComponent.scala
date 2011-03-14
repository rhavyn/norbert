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
package server

import logging.Logging
import jmx.JMX.MBean
import jmx.{FinishedRequestTimeTracker, JMX}
import actors.DaemonActor
import java.util.concurrent.atomic.AtomicInteger
import norbertutils.{SystemClock, NamedPoolThreadFactory}
import java.util.concurrent._
import common.{CachedNetworkStatistics}

/**
 * A component which submits incoming messages to their associated message handler.
 */
trait MessageExecutorComponent {
  val messageExecutor: MessageExecutor
}

trait MessageExecutor {
  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: (Either[Exception, ResponseMsg]) => Unit)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg]): Unit
  def shutdown: Unit
}

class ThreadPoolMessageExecutor(serviceName: String, messageHandlerRegistry: MessageHandlerRegistry, corePoolSize: Int, maxPoolSize: Int,
    keepAliveTime: Int, maxWaitingQueueSize: Int, requestStatisticsWindow: Long) extends MessageExecutor with Logging {

  private val statsActor = CachedNetworkStatistics[Int, Int](SystemClock, requestStatisticsWindow, 200L)

//  val statsActorJMX = JMX.register(new NetworkStatisticsActorMBeanImpl("MessageExecutorStatistics", serviceName, statsActor))

  private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](maxWaitingQueueSize),
    new NamedPoolThreadFactory("norbert-message-executor")) {

    override def beforeExecute(t: Thread, r: Runnable) = {
      val rr = r.asInstanceOf[RequestRunner[_, _]]

      statsActor.beginRequest(0, rr.id)
    }

    override def afterExecute(r: Runnable, t: Throwable) = {
      val rr = r.asInstanceOf[RequestRunner[_, _]]
      statsActor.endRequest(0, rr.id)
    }
  }

  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: (Either[Exception, ResponseMsg]) => Unit)
                                             (implicit is: InputSerializer[RequestMsg, ResponseMsg]) {
    val rr = new RequestRunner(request, responseHandler, is = is)
    try {
      threadPool.execute(rr)
    } catch {
      case ex: RejectedExecutionException =>   throw new HeavyLoadException
      statsActor.endRequest(0, rr.id)
    }
  }

  def shutdown {
    threadPool.shutdown
//    statsActorJMX.foreach { JMX.unregister(_) }
    log.debug("MessageExecutor shut down")
  }

  private val idGenerator = new AtomicInteger(0)

  private class RequestRunner[RequestMsg, ResponseMsg](request: RequestMsg,
                                                       callback: (Either[Exception, ResponseMsg]) => Unit,
                                                       val queuedAt: Long = System.currentTimeMillis,
                                                       val id: Int = idGenerator.getAndIncrement.abs,
                                                       implicit val is: InputSerializer[RequestMsg, ResponseMsg]) extends Runnable {
    def run = {
      log.debug("Executing message: %s".format(request))

      val response: Option[Either[Exception, ResponseMsg]] =
      try {
        val handler = messageHandlerRegistry.handlerFor(request)
        try {
          val response = handler(request)
          if(response == null) None else Some(Right(response))
        } catch {
          case ex: Exception =>
            log.error(ex, "Message handler threw an exception while processing message")
            Some(Left(ex))
        }
      } catch {
        case ex: InvalidMessageException =>
          log.error(ex, "Received an invalid message: %s".format(request))
          Some(Left(ex))


        case ex: Exception =>
          log.error(ex, "Unexpected error while handling message: %s".format(request))
          Some(Left(ex))
      }

      response.foreach(callback)
    }
  }
}

trait RequestProcessorMBean {
  def getQueueSize: Int
  def getAverageWaitTime: Int
  def getAverageProcessingTime: Int
  def getRequestCount: Long
}
