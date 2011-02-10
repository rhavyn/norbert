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
package netty

import java.util.UUID
import org.jboss.netty.channel._
import protos.NorbertProtos
import logging.Logging
import jmx.JMX.MBean
import jmx.JMX
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit, ConcurrentHashMap}
import com.google.protobuf.ByteString

import common.NetworkStatisticsActor
import cluster.Node
import com.linkedin.norbert.network.client.NetworkClientConfig

@ChannelPipelineCoverage("all")
class ClientChannelHandler(serviceName: String, staleRequestTimeoutMins: Int,
        staleRequestCleanupFrequencyMins: Int) extends SimpleChannelHandler with Logging {
  private val requestMap = new ConcurrentHashMap[UUID, Request[_, _]]

  val cleanupTask = new Runnable() {
    val staleRequestTimeoutMillis = TimeUnit.MILLISECONDS.convert(staleRequestTimeoutMins, TimeUnit.MINUTES)

    override def run = {
      try {
        import collection.JavaConversions._
        var expiredEntryCount = 0

        requestMap.keySet.foreach { uuid =>
          val request = requestMap.get(uuid)
          if ((System.currentTimeMillis - request.timestamp) > staleRequestTimeoutMillis) {
            requestMap.remove(uuid)
            expiredEntryCount += 1
          }
        }

        log.info("Expired %d stale entries from the request map".format(expiredEntryCount))
      } catch {
        case e: InterruptedException =>
          Thread.currentThread.interrupt
          log.error(e, "Interrupted exception in cleanup task")
        case e: Exception => log.error(e, "Exception caught in cleanup task, ignoring ")
      }
    }
  }

  val cleanupExecutor = new ScheduledThreadPoolExecutor(1)
  cleanupExecutor.scheduleAtFixedRate(cleanupTask, staleRequestCleanupFrequencyMins, staleRequestCleanupFrequencyMins, TimeUnit.MINUTES)

  private val statsActor = new NetworkStatisticsActor[Int, UUID](100)
  statsActor.start

  private val jmxHandle = JMX.register(new MBean(classOf[NetworkClientStatisticsMBean], "service=%s".format(serviceName)) with NetworkClientStatisticsMBean {
    import statsActor.Stats._

    def getRequestsPerSecond = statsActor !? GetRequestsPerSecond match {
      case RequestsPerSecond(rps) => rps
    }

    def getAverageRequestProcessingTime = statsActor !? GetTotalAverageProcessingTime match {
      case TotalAverageProcessingTime(time) => time
    }

    def getAveragePendingRequestTime = statsActor !? GetTotalAveragePendingTime match {
      case TotalAveragePendingTime(time) => time
    }
  })
  import statsActor.Stats._
  def getAverageRequestProcessingTime(n: Int) = statsActor !? GetTotalAverageProcessingTime match {
      case AverageProcessingTime(times) => times.get(n).getOrElse(NetworkClientConfig.MAXRESPONETIME * 2)
  }

   def getAverageRequestProcessingTime() = statsActor !? GetTotalAverageProcessingTime match {
      case AverageProcessingTime(times) => times
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val request = e.getMessage.asInstanceOf[Request[_, _]]
    log.debug("Writing request: %s".format(request))

    statsActor ! statsActor.Stats.BeginRequest(request.node.id, request.id)

    val message = NorbertProtos.NorbertMessage.newBuilder
    message.setRequestIdMsb(request.id.getMostSignificantBits)
    message.setRequestIdLsb(request.id.getLeastSignificantBits)
    message.setMessageName(request.name)
    message.setMessage(ByteString.copyFrom(request.requestBytes))

    super.writeRequested(ctx, new DownstreamMessageEvent(e.getChannel, e.getFuture, message.build, e.getRemoteAddress))
  }

  def updateNodeHealth (node: Node) = {
    node.setHealth(NetworkClientConfig.calculateScore(getAverageRequestProcessingTime(node.id)))
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val message = e.getMessage.asInstanceOf[NorbertProtos.NorbertMessage]
    log.debug("Received message: %s".format(message))
    val requestId = new UUID(message.getRequestIdMsb, message.getRequestIdLsb)

    requestMap.get(requestId) match {
      case null => log.warn("Received a response message [%s] without a corresponding request".format(message))
      case request =>
        requestMap.remove(requestId)

        statsActor ! statsActor.Stats.EndRequest(request.node.id, request.id)
        updateNodeHealth(request.node)
        if (message.getStatus == NorbertProtos.NorbertMessage.Status.OK) {
          request.processResponseBytes(message.getMessage.toByteArray)
        } else {
          val errorMsg = if (message.hasErrorMessage()) message.getErrorMessage else "<null>"
          val errorName = message.getMessageName
          if (errorName == "HeavyLoadException")  {
            // mark the node offline a period of time
            
          }
          request.processException(new RemoteException(message.getMessageName, message.getErrorMessage))
        }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.info(e.getCause, "Caught exception in network layer")

  def shutdown: Unit = jmxHandle.foreach { JMX.unregister(_) }
}



trait NetworkClientStatisticsMBean {
  def getRequestsPerSecond: Int
  def getAverageRequestProcessingTime: Int
  def getAveragePendingRequestTime: Int
}
