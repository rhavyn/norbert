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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}
import util.NamedPoolThreadFactory
import logging.Logging
import jmx.JMX.MBean
import jmx.{AverageTimeTracker, JMX}
import actors.DaemonActor

/**
 * A component which submits incoming messages to their associated message handler.
 */
trait MessageExecutorComponent {
  val messageExecutor: MessageExecutor
}

trait MessageExecutor {
  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: (Either[Exception, ResponseMsg]) => Unit)
  (implicit serializer: Serializer[RequestMsg, ResponseMsg]): Unit
  def shutdown: Unit
}

class ThreadPoolMessageExecutor(messageHandlerRegistry: MessageHandlerRegistry, corePoolSize: Int, maxPoolSize: Int,
    keepAliveTime: Int) extends MessageExecutor with Logging {
  private val statsActor = new DaemonActor {
    private val waitTime = new AverageTimeTracker(100)
    private val processingTime = new AverageTimeTracker(100)
    private var requestCount = 0L

    def act() = {
      import Stats._

      loop {
        react {
          case NewRequest(time) =>
            waitTime.addTime(time)
            requestCount += 1

          case NewProcessingTime(time) => processingTime.addTime(time)

          case GetAverageWaitTime => reply(AverageWaitTime(waitTime.average))

          case GetAverageProcessingTime => reply(AverageProcessingTime(processingTime.average))

          case GetRequestCount => reply(RequestCount(requestCount))

          case msg => log.error("Stats actor got invalid message: %s".format(msg))
        }
      }
    }
  }
  statsActor.start

  private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable],
    new NamedPoolThreadFactory("norbert-message-executor")) {
    private val startedAt = new ThreadLocal[Long]

    override def beforeExecute(t: Thread, r: Runnable) = {
      val rr = r.asInstanceOf[RequestRunner[_, _]]
      val ts = System.currentTimeMillis
      statsActor ! Stats.NewRequest((ts - rr.queuedAt).toInt)
      startedAt.set(ts)
    }

    override def afterExecute(r: Runnable, t: Throwable) = {
      val ts = startedAt.get
      startedAt.remove
      statsActor ! Stats.NewProcessingTime((System.currentTimeMillis - ts).toInt)
    }
  }

  private val jmxHandle = JMX.register(new MBean(classOf[RequestProcessorMBean]) with RequestProcessorMBean {
    import Stats._

    def getQueueSize = threadPool.getQueue.size

    def getAverageWaitTime = statsActor !? GetAverageWaitTime match {
      case AverageWaitTime(t) => t
    }

    def getAverageProcessingTime = statsActor !? GetAverageProcessingTime match {
      case AverageProcessingTime(t) => t
    }

    def getRequestCount = statsActor !? GetRequestCount match {
      case RequestCount(c) => c
    }
  })


  def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: (Either[Exception, ResponseMsg]) => Unit)(implicit serializer: Serializer[RequestMsg, ResponseMsg]) {
    threadPool.execute(new RequestRunner(request, responseHandler, serializer = serializer))
  }

  def shutdown {
    jmxHandle.foreach { JMX.unregister(_) }
    threadPool.shutdown
    log.debug("MessageExecutor shut down")
  }

  private class RequestRunner[RequestMsg, ResponseMsg](request: RequestMsg, callback: (Either[Exception, ResponseMsg]) => Unit, val queuedAt: Long = System.currentTimeMillis, implicit val serializer: Serializer[RequestMsg, ResponseMsg]) extends Runnable {
    def run = {
      log.debug("Executing message: %s".format(request))

      val response: Option[Either[Exception, ResponseMsg]] =
      try {
        val handler = messageHandlerRegistry.handlerFor(request)
        println(handler)
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

  private object Stats {
    case class NewRequest(waitTime: Int)
    case object GetAverageWaitTime
    case class AverageWaitTime(time: Int)
    case class NewProcessingTime(time: Int)
    case object GetAverageProcessingTime
    case class AverageProcessingTime(time: Int)
    case object GetRequestCount
    case class RequestCount(count: Long)
  }
}

trait RequestProcessorMBean {
  def getQueueSize: Int
  def getAverageWaitTime: Int
  def getAverageProcessingTime: Int
  def getRequestCount: Long
}
