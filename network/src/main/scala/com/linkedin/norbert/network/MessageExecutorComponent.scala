package com.linkedin.norbert.network

import com.google.protobuf.Message
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}

trait MessageExecutorComponent {
  this: MessageRegistryComponent =>

  val messageExecutor: MessageExecutor

  class MessageExecutor(corePoolSize: Int, maxPoolSize: Int, keepAliveTime: Int) {
    private val threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue)

    def executeMessage(message: Message, responseHandler: (Either[Exception, Message]) => Unit): Unit = {
      threadPool.execute(new Runnable  {
        def run {
          messageRegistry.handlerForClassName(message.getClass.getName) match {
            case Some(handler) => try {
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
  }
}
