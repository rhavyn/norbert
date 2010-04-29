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

import com.google.protobuf.Message
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}
import util.NamedPoolThreadFactory
import logging.Logging

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

  def executeMessage(message: Message, responseHandler: (Either[Exception, Message]) => Unit): Unit = {
    threadPool.execute(new Runnable  {
      def run {
        try {
          log.ifDebug("Executing message: %s", message)
          val handler = messageHandlerRegistry.handlerFor(message)

          try {
            val response = handler(message)

            if (messageHandlerRegistry.validResponseFor(message, response)) {
              if (response != null) responseHandler(Right(response))
            } else {
              val name = if (response == null) "<null>" else response.getDescriptorForType.getFullName
              val errorMsg = "Message handler returned an invalid response message of type %s".format(name)
              log.error(errorMsg)
              responseHandler(Left(new InvalidMessageException(errorMsg)))
            }
          } catch {
            case ex: Exception =>
              log.error(ex, "Message handler threw an exception while processing message")
              responseHandler(Left(ex))
          }
        } catch {
          case ex: InvalidMessageException => log.error(ex, "Received an invalid message: %s", message)
          case ex: Exception => log.error(ex, "Unexpected error while handling message: %s", message)
        }
      }
    })
  }

  def shutdown {
    threadPool.shutdown
    log.ifDebug("MessageExecutor shut down")
  }
}
