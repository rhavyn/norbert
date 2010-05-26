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
package com.linkedin.norbert.network.server

import com.google.protobuf.Message
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.InvalidMessageException
import com.linkedin.norbert.util.NamedPoolThreadFactory
import com.linkedin.norbert.jmx.JMX.MBean
import com.linkedin.norbert.jmx.JMX

/**
 * A component which submits incoming messages to their associated message handler.
 */
trait MessageExecutorComponent {
  val messageExecutor: MessageExecutor
}

trait MessageExecutor {
  def executeMessage(message: Message, responseHandler: (Either[Exception, Message]) => Unit): Unit
  def shutdown: Unit
}

class ThreadPoolMessageExecutor(messageHandlerRegistry: MessageHandlerRegistry, corePoolSize: Int, maxPoolSize: Int,
    keepAliveTime: Int) extends MessageExecutor with Logging {
  private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable],
    new NamedPoolThreadFactory("norbert-message-executor"))

  JMX.register(new MBean(classOf[RequestProcessorMBean]) with RequestProcessorMBean {
    def getQueueSize = threadPool.getQueue.size
    def getAverageWaitTime = 0
    def getAverageProcessingTime = 0
  })

  def executeMessage(message: Message, responseHandler: (Either[Exception, Message]) => Unit): Unit = {
    threadPool.execute(new RequestRunner(message, responseHandler))
  }

  def shutdown {
    threadPool.shutdown
    log.debug("MessageExecutor shut down")
  }

  private class RequestRunner(message: Message, responseHandler: (Either[Exception, Message]) => Unit, queuedAt: Long = System.currentTimeMillis) extends Runnable {
    def run = {
      log.debug("Executing message: %s".format(message))

      val response: Option[Either[Exception, Message]] = try {
        val handler = messageHandlerRegistry.handlerFor(message)

        try {
          val response = handler(message)
          if (messageHandlerRegistry.validResponseFor(message, response)) {
            if (response == null) None else Some(Right(response))
          } else {
            val name = if (response == null) "<null>" else response.getDescriptorForType.getFullName
            val errorMsg = "Message handler returned an invalid response message of type %s".format(name)
            log.error(errorMsg)
            Some(Left(new InvalidMessageException(errorMsg)))
          }
        } catch {
          case ex: Exception =>
            log.error(ex, "Message handler threw an exception while processing message")
            Some(Left(ex))
        }
      } catch {
        case ex: InvalidMessageException =>
          log.error(ex, "Received an invalid message: %s".format(message))
          Some(Left(ex))

        case ex: Exception =>
          log.error(ex, "Unexpected error while handling message: %s".format(message))
          Some(Left(ex))
      }

      response.foreach(responseHandler)
    }
  }
}

trait RequestProcessorMBean {
  def getQueueSize: Int
  def getAverageWaitTime: Int
  def getAverageProcessingTime: Int
}
