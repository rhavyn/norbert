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

import com.google.protobuf.Message
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}
import com.linkedin.norbert.util.Logging

trait MessageExecutorComponent {
  this: MessageRegistryComponent =>

  def messageExecutor: MessageExecutor

  trait MessageExecutor {
    def executeMessage(message: Message, responseHandler: (Either[Exception, Message]) => Unit): Unit
    def shutdown: Unit
  }

  class DoNothingMessageExecutor extends MessageExecutor {
    def executeMessage(message: Message, responseHandler: (Either[Exception, Message]) => Unit): Unit = null
    def shutdown: Unit = null
  }

  class ThreadPoolMessageExecutor(corePoolSize: Int, maxPoolSize: Int, keepAliveTime: Int) extends MessageExecutor with Logging {
    private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue)

    def executeMessage(message: Message, responseHandler: (Either[Exception, Message]) => Unit): Unit = {
      threadPool.execute(new Runnable  {
        def run {
          messageRegistry.handlerForClassName(message.getClass.getName) match {
            case Some(handler) => try {
              log.ifDebug("Executing message: %s[%s]", message.getClass.getName, message)
              handler(message) match {
                case Some(m) => responseHandler(Right(m))
                case None => // do nothing
              }
            } catch {
              case ex: Exception => responseHandler(Left(ex))
            }

            case None => responseHandler(Left(new InvalidMessageException("Message does have a registered handler: " + message.getClass.getName)))
          }
        }
      })
    }

    def shutdown {
      threadPool.shutdown
      log.ifDebug("MessageExecutor shut down")
    }
  }
}
