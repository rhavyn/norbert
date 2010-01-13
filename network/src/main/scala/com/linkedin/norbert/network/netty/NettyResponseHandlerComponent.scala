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
package com.linkedin.norbert.network.netty

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, ConcurrentMap, ConcurrentHashMap}
import org.jboss.netty.channel._
import java.util.UUID
import com.linkedin.norbert.util.Logging
import com.linkedin.norbert.network._
import com.linkedin.norbert.protos.NorbertProtos

trait NettyResponseHandlerComponent extends ResponseHandlerComponent {
  this: MessageRegistryComponent =>
  
  def requestCleanupFrequency = NetworkDefaults.REQUEST_CLEANUP_FREQUENCY
  def requestTimeout = NetworkDefaults.REQUEST_TIMEOUT

  override val responseHandler: NettyResponseHandler

  @ChannelPipelineCoverage("all")
  class NettyResponseHandler extends SimpleChannelHandler with ResponseHandler with Logging {
    private val requestMap = new ConcurrentHashMap[UUID, (Request, AtomicInteger)]
    private val cleanupThread = new RequestCleanupThread(requestMap)
    cleanupThread.setDaemon(true)
    cleanupThread.start

    override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
      val request = e.getMessage.asInstanceOf[Request]
      log.ifDebug("Noting request: %s", request)
      requestMap.put(request.id, (request, new AtomicInteger(request.numResponses)))

      val message = NorbertProtos.NorbertMessage.newBuilder
      message.setRequestIdMsb(request.id.getMostSignificantBits)
      message.setRequestIdLsb(request.id.getLeastSignificantBits)
      message.setMessageName(request.message.getClass.getName)
      message.setMessage(request.message.toByteString)

      super.writeRequested(ctx, new DownstreamMessageEvent(e.getChannel, e.getFuture, message.build, e.getRemoteAddress))
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
      val norbertMessage = e.getMessage.asInstanceOf[NorbertProtos.NorbertMessage]
      log.ifDebug("Received message: %s", norbertMessage)
      val requestId = new UUID(norbertMessage.getRequestIdMsb, norbertMessage.getRequestIdLsb)

      doWithRequest(requestId, norbertMessage, e.getChannel) { request =>
        handleResponse(request, norbertMessage)
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.error(e.getCause, "Caught exception in networking code")

    private def doWithRequest(requestId: UUID, norbertMessage: NorbertProtos.NorbertMessage, channel: Channel)(block: (Request) => Unit) {
      requestMap.get(requestId) match {
        case null => log.error("Received a response with id %d and class %s without a corresponding request from %s",
          requestId, norbertMessage.getMessageName, channel)

        case (request, numResponses) =>
          log.ifDebug("Found request with id: %s", requestId)
          try {
            block(request)
          } catch {
            case ex: Exception => log.error(ex, "Uncaught exception while processing request")
          } finally {
            if (numResponses.decrementAndGet == 0) {
              log.ifDebug("Received all responses for id %s, clearing noted request", requestId)
              requestMap.remove(requestId)
            }
          }
      }
    }
  }

  private class RequestCleanupThread(requestMap: ConcurrentMap[UUID, (Request, AtomicInteger)]) extends Thread("request-cleanup-thread") with Logging {
    override def run: Unit = {
      import collection.jcl.Conversions._

      log.ifDebug("Starting request cleanup thread")
      
      while (true) {
        try {
          TimeUnit.MINUTES.sleep(requestCleanupFrequency)
          log.ifDebug("Running request cleanup")
          requestMap.keySet.foreach {uuid =>
            requestMap.get(uuid) match {
              case null => // do nothing
              case (request, _) =>
                if (System.currentTimeMillis - request.timestamp > TimeUnit.MINUTES.toMillis(requestTimeout)) {
                  log.ifDebug("Removing request with id: %s", uuid)
                  requestMap.remove(uuid)
                }
            }
          }
        }
        catch {
          case ex: Exception => log.error(ex, "Uncaught exception")
        }
      }
    }
  }
}
