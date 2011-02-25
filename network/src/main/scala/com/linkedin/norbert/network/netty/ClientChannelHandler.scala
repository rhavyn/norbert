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
import norbertutils._
import network.client.ResponseHandler
import norbertutils.{Clock, SystemClock, SystemClockComponent}
import java.util.concurrent.atomic.AtomicLong

@ChannelPipelineCoverage("all")
class ClientChannelHandler(serviceName: String,
                           staleRequestTimeoutMins: Int,
                           staleRequestCleanupFrequencyMins: Int,
                           requestStatisticsWindow: Long,
                           outlierMultiplier: Int,
                           outlierConstant: Int,
                           responseHandler: ResponseHandler) extends SimpleChannelHandler with Logging {
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
            statsActor ! statsActor.Stats.EndRequest(request.node, request.id)
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

  private val statsActor = new NetworkStatisticsActor[Node, UUID](clock, requestStatisticsWindow)
  statsActor.start

  val clientStatsStrategy = Some(new ClientStatisticsRequestStrategy(statsActor, outlierMultiplier, outlierConstant, clock))
  val serverErrorStrategy = Some(new SimpleBackoffStrategy(clock))

  val clientStatsStrategyJMX = JMX.register(clientStatsStrategy.map(new ClientStatisticsRequestStrategyMBeanImpl(serviceName, _)))
  val serverErrorStrategyJMX = JMX.register(serverErrorStrategy.map(new ServerErrorStrategyMBeanImpl(serviceName, _)))

  val strategy = CompositeCanServeRequestStrategy.build(clientStatsStrategy, serverErrorStrategy)

  private val statsJMX = JMX.register(new NetworkClientStatisticsMBeanImpl(serviceName, statsActor))

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val request = e.getMessage.asInstanceOf[Request[_, _]]
    log.debug("Writing request: %s".format(request))

    requestMap.put(request.id, request)
    statsActor ! statsActor.Stats.BeginRequest(request.node, request.id)

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

        statsActor ! statsActor.Stats.EndRequest(request.node, request.id)

        if (message.getStatus == NorbertProtos.NorbertMessage.Status.OK) {
          responseHandler.onSuccess(request, message)
        } else if (message.getStatus == NorbertProtos.NorbertMessage.Status.HEAVYLOAD) {
          serverErrorStrategy.foreach(_.notifyFailure(request.node))
          processException(request, "Heavy load")
        } else {
          processException(request, Option(message.getErrorMessage).getOrElse("<null>"))
        }
    }

    def processException[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg], errorMessage: String) {
      responseHandler.onFailure(request, new RemoteException(request.name, errorMessage))
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.warn(e.getCause, "Caught exception in network layer")

  def shutdown: Unit = {
    statsJMX.foreach { JMX.unregister(_) }
    serverErrorStrategyJMX.foreach { JMX.unregister(_) }
    clientStatsStrategyJMX.foreach { JMX.unregister(_) }
  }
}

class ClientStatisticsRequestStrategy(statsActor: NetworkStatisticsActor[Node, UUID], outlierMultiplier: Int, outlierConstant: Int, clock: Clock, refreshInterval: Long = 200L)
  extends CanServeRequestStrategy with Logging {
  // Must be more than outlierMultiplier * average + outlierConstant ms the others by default

  @volatile var canServeRequests = Map.empty[Node, Boolean]
  val lastUpdateTime = new AtomicLong(0)

  def canServeRequest(node: Node): Boolean = {
    val lut = lastUpdateTime.get
    if(clock.getCurrentTime - lut > refreshInterval) {
      if(lastUpdateTime.compareAndSet(lut, clock.getCurrentTime))
        statsActor !! (statsActor.Stats.GetProcessingStatistics, {
          case statsActor.Stats.ProcessingStatistics(map) =>
            // We now have a map from node_id => statistics. Add up the process
            val totalTime = map.values.map(_.completedTime).sum + map.values.map(_.pendingTime).sum
            val totalSize = map.values.map(_.completedSize).sum + map.values.map(_.pendingSize).sum

            canServeRequests = map.map { case (n, entry) =>
              val nodeTime = entry.completedTime + entry.pendingTime
              val nodeSize = entry.completedSize + entry.pendingSize

              //    (nodeTime) / (nodeSize)  < (totalTime) / (totalSize) * OUTLIER_MULTIPLIER + OUTLIER_CONSTANT
              val available = nodeTime * totalSize <= (totalTime * outlierMultiplier + outlierConstant) * nodeSize

              if(!available) {
                val nodeAverage = safeDivide(nodeTime, nodeSize)(0)
                val clusterAverage = safeDivide(totalTime, totalSize)(0)

                log.warn("Node %s has an average response time of %f. The cluster response time is %f. Routing requests away temporarily.".format(n, nodeAverage, clusterAverage))
              }
              (n, available)
            }
        })
    }

    canServeRequests.getOrElse(node, true)
  }
}

class ClientStatisticsRequestStrategyMBeanImpl(serviceName: String, strategy: ClientStatisticsRequestStrategy)
  extends MBean(classOf[ClientStatisticsRequestStrategyMBean], "service=%s".format(serviceName))
  with ClientStatisticsRequestStrategyMBean {

  def canServeRequests = strategy.canServeRequests.map { case (n, a) => (n.id -> a) }
}

trait NetworkClientStatisticsMBean {
  def getNumPendingRequests: Map[Int, Int]

  def getMedianTimes: Map[Int, Int]
  def get75thTimes: Map[Int, Int]
  def get90thTimes: Map[Int, Int]
  def get95thTimes: Map[Int, Int]
  def get99thTimes: Map[Int, Int]

  def getClusterRequestsPerSecond: Int
  def getClusterAverageTime: Double
  def getClusterPendingTime: Double

  def getClusterMedianTimes: Double
  def getCluster75thTimes: Double
  def getCluster90th: Double
  def getCluster95th: Double
  def getCluster99th: Double

  def reset
}

class NetworkClientStatisticsMBeanImpl(serviceName: String, statsActor: NetworkStatisticsActor[Node, UUID])
  extends MBean(classOf[NetworkClientStatisticsMBean], "service=%s".format(serviceName))
  with NetworkClientStatisticsMBean {
  import statsActor.Stats._

  private def getProcessingStatistics(percentile: Option[Double] = None) =
    statsActor !? GetProcessingStatistics(percentile) match {
      case ProcessingStatistics(map) => map.map { case (n, s) => (n.id -> s) }
    }

  def getClusterRequestsPerSecond = statsActor !? GetRequestsPerSecond match {
    case RequestsPerSecond(rps) => rps
  }

  def getNumPendingRequests = getProcessingStatistics().mapValues(_.pendingSize)

  def getMedianTimes =
    getProcessingStatistics(Some(0.5)).mapValues(_.percentile.getOrElse(0))

  def get75thTimes =
    getProcessingStatistics(Some(0.75)).mapValues(_.percentile.getOrElse(0))

  def get90thTimes =
    getProcessingStatistics(Some(0.90)).mapValues(_.percentile.getOrElse(0))

  def get95thTimes =
    getProcessingStatistics(Some(0.95)).mapValues(_.percentile.getOrElse(0))

  def get99thTimes =
    getProcessingStatistics(Some(0.99)).mapValues(_.percentile.getOrElse(0))

  def ave[K, V : Numeric](map: Map[K, V]) = average(map.values.sum, map.size)

  def getClusterAverageTime = average(getProcessingStatistics()){_.completedTime}{_.completedSize}

  def getClusterPendingTime = average(getProcessingStatistics()){_.pendingTime}{_.pendingSize}

  def getClusterMedianTimes = ave(getMedianTimes)

  def getCluster75thTimes = ave(get75thTimes)

  def getCluster90th = ave(get90thTimes)

  def getCluster95th = ave(get95thTimes)

  def getCluster99th = ave(get99thTimes)

  def reset = statsActor ! Reset
}

