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
import java.util.{Map => JMap}

@ChannelPipelineCoverage("all")
class ClientChannelHandler(serviceName: String,
                           staleRequestTimeoutMins: Int,
                           staleRequestCleanupFrequencyMins: Int,
                           requestStatisticsWindow: Long,
                           outlierMultiplier: Double,
                           outlierConstant: Double,
                           responseHandler: ResponseHandler) extends SimpleChannelHandler with Logging {
  private val requestMap = new ConcurrentHashMap[UUID, Request[_, _]]

  val cleanupTask = new Runnable() {
    val staleRequestTimeoutMillis = TimeUnit.MILLISECONDS.convert(staleRequestTimeoutMins, TimeUnit.MINUTES)

    override def run = {
      try {
        import collection.JavaConversions._
        var expiredEntryCount = 0

        requestMap.keySet.foreach { uuid =>
          val request = Option(requestMap.get(uuid))
          val now = System.currentTimeMillis

          request.foreach { r =>
             if ((now - r.timestamp) > staleRequestTimeoutMillis) {
                requestMap.remove(uuid)
                statsActor ! statsActor.Stats.EndRequest(r.node, r.id)
                expiredEntryCount += 1
             }
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

  val clientStatsStrategy = new ClientStatisticsRequestStrategy(statsActor, outlierMultiplier, outlierConstant, clock)
  val serverErrorStrategy = new SimpleBackoffStrategy(clock)

  val clientStatsStrategyJMX = JMX.register(new ClientStatisticsRequestStrategyMBeanImpl(serviceName, clientStatsStrategy))
  val serverErrorStrategyJMX = JMX.register(new ServerErrorStrategyMBeanImpl(serviceName, serverErrorStrategy))

  val strategy = CompositeCanServeRequestStrategy(clientStatsStrategy, serverErrorStrategy)

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
          serverErrorStrategy.notifyFailure(request.node)
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

trait HealthScoreCalculator {
  val statsActor: NetworkStatisticsActor[Node, UUID] // TODO: Annoying path dependant types

  def doCalculation(values: Iterable[statsActor.Stats.ProcessingEntry]): Double = {
    def completedMedian = safeDivide(values.flatMap(_.completedPercentile).sum, values.size)(0)
    def pendingMedian = safeDivide(values.flatMap(_.pendingPercentile).sum, values.size)(0)

    def completedSize = values.map(_.completedSize).sum
    def pendingSize = values.map(_.pendingSize).sum

    // Average between the completed and pending medians, based on how many of each there are
    safeDivide(completedMedian * completedSize + pendingMedian * pendingSize, completedSize + pendingSize)(0)
  }
}

class ClientStatisticsRequestStrategy(val statsActor: NetworkStatisticsActor[Node, UUID],
                                      @volatile var outlierMultiplier: Double,
                                      @volatile var outlierConstant: Double,
                                      clock: Clock,
                                      refreshInterval: Long = 200L)
  extends CanServeRequestStrategy with Logging with HealthScoreCalculator {
  // Must be more than outlierMultiplier * average + outlierConstant ms the others by default

  @volatile var canServeRequests = Map.empty[Node, Boolean]
  val lastUpdateTime = new AtomicLong(0)

  def canServeRequest(node: Node): Boolean = {
    val lut = lastUpdateTime.get
    if(clock.getCurrentTime - lut > refreshInterval) {
      if(lastUpdateTime.compareAndSet(lut, clock.getCurrentTime))
        statsActor !! (statsActor.Stats.GetProcessingStatistics(Some(0.5)), {
          case statsActor.Stats.ProcessingStatistics(map) =>

            val clusterMedian = doCalculation(map.values)

            canServeRequests = map.map { case (n, entry) =>
              val nodeMedian = doCalculation(List(entry))

              val available = nodeMedian <= clusterMedian * outlierMultiplier + outlierConstant

              if(!available) {
                log.warn("Node %s has a median response time of %f. The cluster response time is %f. Routing requests away temporarily.".format(n, nodeMedian, clusterMedian))
              }
              (n, available)
            }
        })
    }

    canServeRequests.getOrElse(node, true)
  }
}

trait ClientStatisticsRequestStrategyMBean extends CanServeRequestStrategyMBean {
  def getOutlierMultiplier: Double
  def getOutlierConstant: Double

  def setOutlierMultiplier(m: Double)
  def setOutlierConstant(c: Double)
}

class ClientStatisticsRequestStrategyMBeanImpl(serviceName: String, strategy: ClientStatisticsRequestStrategy)
  extends MBean(classOf[ClientStatisticsRequestStrategyMBean], "service=%s".format(serviceName))
  with ClientStatisticsRequestStrategyMBean {

  def getCanServeRequests = toJMap(strategy.canServeRequests.map { case (n, a) => (n.id -> a) })

  def getOutlierMultiplier = strategy.outlierMultiplier

  def getOutlierConstant = strategy.outlierConstant

  def setOutlierMultiplier(m: Double) { strategy.outlierMultiplier = m}

  def setOutlierConstant(c: Double) = { strategy.outlierConstant = c}
}

trait NetworkClientStatisticsMBean {
  def getNumPendingRequests: JMap[Int, Int]

  def getMedianTimes: JMap[Int, Double]
  def get75thTimes: JMap[Int, Double]
  def get90thTimes: JMap[Int, Double]
  def get95thTimes: JMap[Int, Double]
  def get99thTimes: JMap[Int, Double]
  def getHealthScoreTimings: JMap[Int, Double]

  def getRPS: JMap[Int, Int]

  def getClusterRPS: Int
  def getClusterAverageTime: Double
  def getClusterPendingTime: Double

  def getClusterMedianTime: Double
  def getCluster75thTimes: Double
  def getCluster90th: Double
  def getCluster95th: Double
  def getCluster99th: Double
  def getClusterHealthScoreTiming: Double

  def reset

  // Jill will be very upset if I break her graphs
  def getRequestsPerSecond = getClusterRPS
  def getAverageRequestProcessingTime = getClusterAverageTime
}

class NetworkClientStatisticsMBeanImpl(serviceName: String, val statsActor: NetworkStatisticsActor[Node, UUID])
  extends MBean(classOf[NetworkClientStatisticsMBean], "service=%s".format(serviceName))
  with NetworkClientStatisticsMBean with HealthScoreCalculator {
  import statsActor.Stats._

  private def getProcessingStatistics(percentile: Option[Double] = None) =
    statsActor !? GetProcessingStatistics(percentile) match {
      case ProcessingStatistics(map) => map.map { case (n, s) => (n.id -> s) }
    }

  private def getRps = statsActor !? GetRequestsPerSecond match {
    case RequestsPerSecond(rps) => rps
  }

  def getNumPendingRequests = toJMap(getProcessingStatistics().mapValues(_.pendingSize))

  def getMedianTimes =
    toJMap(getProcessingStatistics(Some(0.5)).mapValues(_.completedPercentile.getOrElse(0.0)))

  def get75thTimes =
    toJMap(getProcessingStatistics(Some(0.75)).mapValues(_.completedPercentile.getOrElse(0)))

  def get90thTimes =
    toJMap(getProcessingStatistics(Some(0.90)).mapValues(_.completedPercentile.getOrElse(0)))

  def get95thTimes =
    toJMap(getProcessingStatistics(Some(0.95)).mapValues(_.completedPercentile.getOrElse(0)))

  def get99thTimes =
    toJMap(getProcessingStatistics(Some(0.99)).mapValues(_.completedPercentile.getOrElse(0)))

  def getHealthScoreTimings = {
    toJMap(getProcessingStatistics(Some(0.5)).mapValues(entry => doCalculation(List(entry))))
  }

  def getRPS = toJMap(getRps.map { case (n, r) => (n.id -> r) })

  def ave[K, V : Numeric](map: JMap[K, V]) = {
    import scala.collection.JavaConversions._
    average(map.values.sum, map.size)
  }

  def getClusterAverageTime = average(getProcessingStatistics()){_.completedTime}{_.completedSize}

  def getClusterPendingTime = average(getProcessingStatistics()){_.pendingTime}{_.pendingSize}

  def getClusterMedianTime = ave(getMedianTimes)

  def getCluster75thTimes = ave(get75thTimes)

  def getCluster90th = ave(get90thTimes)

  def getCluster95th = ave(get95thTimes)

  def getCluster99th = ave(get99thTimes)

  def getClusterRPS = statsActor !? GetRequestsPerSecond match {
    case RequestsPerSecond(rps) => rps.values.sum
  }

  def getClusterHealthScoreTiming = doCalculation(getProcessingStatistics(Some(0.5)).values)

  def reset = statsActor ! Reset
}

