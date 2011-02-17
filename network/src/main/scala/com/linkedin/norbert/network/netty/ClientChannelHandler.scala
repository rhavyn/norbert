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
import cluster.Node
import scala.math._
import client.NetworkClientConfig
import common._
import util.{Clock, SystemClock, SystemClockComponent}

@ChannelPipelineCoverage("all")
class ClientChannelHandler(serviceName: String, staleRequestTimeoutMins: Int,         
        staleRequestCleanupFrequencyMins: Int, requestStatisticsWindow: Long, outlierMultiplier: Int, outlierConstant: Int) extends SimpleChannelHandler with Logging {
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
            statsActor ! statsActor.Stats.EndRequest(request.node.id, request.id)
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

  val clock = SystemClock

  val cleanupExecutor = new ScheduledThreadPoolExecutor(1)
  cleanupExecutor.scheduleAtFixedRate(cleanupTask, staleRequestCleanupFrequencyMins, staleRequestCleanupFrequencyMins, TimeUnit.MINUTES)

  private val statsActor = new NetworkStatisticsActor[Int, UUID](clock, requestStatisticsWindow)
  statsActor.start

  val clientStatisticsRequestStrategy = new ClientStatisticsRequestStrategy(statsActor, outlierMultiplier, outlierConstant, clock)
  val serverErrorStrategy = new ServerErrorStrategy(clock)

  val strategy = CompositeCanServeRequestStrategy(clientStatisticsRequestStrategy, serverErrorStrategy)

  private val jmxHandle = JMX.register(new MBean(classOf[NetworkClientStatisticsMBean], "service=%s".format(serviceName)) with NetworkClientStatisticsMBean {
    import statsActor.Stats._

    def getRequestsPerSecond = statsActor !? GetRequestsPerSecond match {
      case RequestsPerSecond(rps) => rps
    }

    def getAverageRequestProcessingTime = statsActor !? GetProcessingStatistics match {
      case ProcessingStatistics(map) => average(map){_.completedTime}{_.completedSize}
    }

    def getAveragePendingRequestTime = statsActor !? GetProcessingStatistics match {
      case ProcessingStatistics(map) => average(map){_.pendingTime}{_.pendingSize}
    }
  })

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val request = e.getMessage.asInstanceOf[Request[_, _]]
    log.debug("Writing request: %s".format(request))

    requestMap.put(request.id, request)
    statsActor ! statsActor.Stats.BeginRequest(request.node.id, request.id)

    val message = NorbertProtos.NorbertMessage.newBuilder
    message.setRequestIdMsb(request.id.getMostSignificantBits)
    message.setRequestIdLsb(request.id.getLeastSignificantBits)
    message.setMessageName(request.name)
    message.setMessage(ByteString.copyFrom(request.requestBytes))

    super.writeRequested(ctx, new DownstreamMessageEvent(e.getChannel, e.getFuture, message.build, e.getRemoteAddress))
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

        if (message.getStatus == NorbertProtos.NorbertMessage.Status.OK) {
          request.processResponseBytes(message.getMessage.toByteArray)
        } else if (message.getStatus == NorbertProtos.NorbertMessage.Status.HEAVYLOAD) {
          serverErrorStrategy.notifyFailure(request.node.id)
        } else {
          val errorMsg = if (message.hasErrorMessage()) message.getErrorMessage else "<null>"
          request.processException(new RemoteException(message.getMessageName, message.getErrorMessage))
        }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.warn(e.getCause, "Caught exception in network layer")

  def shutdown: Unit = {
    jmxHandle.foreach { JMX.unregister(_) }
  }
}

class ClientStatisticsRequestStrategy(statsActor: NetworkStatisticsActor[Int, UUID], outlierMultiplier: Int, outlierConstant: Int, clock: Clock, refreshInterval: Long = 3000L) extends CanServeRequestStrategy {
  // Must be more than 2x + 10ms the others by default

  var canServeRequests = Map.empty[Int, Boolean]
  @volatile var lastUpdateTime = 0L

  def canServeRequest(node: Node): Boolean = {
    if(clock.getCurrentTime - lastUpdateTime > refreshInterval) {
      lastUpdateTime = clock.getCurrentTime

      statsActor !! (statsActor.Stats.GetProcessingStatistics, {
        case statsActor.Stats.ProcessingStatistics(map) =>
          // We now have a map from node_id => statistics. Add up the process
          val totalTime = map.values.map(_.completedTime).sum + map.values.map(_.pendingTime).sum
          val totalSize = map.values.map(_.completedSize).sum + map.values.map(_.pendingSize).sum

          canServeRequests = map.map { case (nodeId, entry) =>
            val nodeTime = entry.completedTime + entry.pendingTime
            val nodeSize = entry.completedSize + entry.pendingSize

            //    (nodeTime) / (nodeSize)  < (totalTime) / (totalSize) * OUTLIER_MULTIPLIER + OUTLIER_CONSTANT
            (nodeId, nodeTime * totalSize <= (totalTime * outlierMultiplier + outlierConstant) * nodeSize)
          }
      })
    }

    canServeRequests.getOrElse(node.id, true)
  }

  def calcVariance(values: Iterable[Int], sum: Int) = {
    lazy val average = sum.toDouble / values.size
    values.foldLeft(0.0) { (sum, value) => sum + ((value - average) * (value - average)) }
  }

  def calcStandardDeviation(values: Iterable[Int], variance: Double): Double = {
    if(values.size == 0 || values.size == 1)
      0
    else {
      sqrt(variance / (values.size - 1))
    }
  }

  def calcStandardDeviation(values: Iterable[Int]): Double = {
    calcStandardDeviation(values, calcVariance(values, values.sum))
  }
}



trait NetworkClientStatisticsMBean {
  def getRequestsPerSecond: Int
  def getAverageRequestProcessingTime: Double
  def getAveragePendingRequestTime: Double
}
