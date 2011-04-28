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
import java.util.{Map => JMap}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import util.ProtoUtils

@ChannelPipelineCoverage("all")
class ClientChannelHandler(clientName: Option[String],
                           serviceName: String,
                           staleRequestTimeoutMins: Int,
                           staleRequestCleanupFrequencyMins: Int,
                           requestStatisticsWindow: Long,
                           outlierMultiplier: Double,
                           outlierConstant: Double,
                           responseHandler: ResponseHandler,
                           avoidByteStringCopy: Boolean) extends SimpleChannelHandler with Logging {
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
                stats.endRequest(r.node, r.id)
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

  private val stats = CachedNetworkStatistics[Node, UUID](clock, requestStatisticsWindow, 200L)

  val clientStatsStrategy = new ClientStatisticsRequestStrategy(stats, outlierMultiplier, outlierConstant, clock)
  val serverErrorStrategy = new SimpleBackoffStrategy(clock)

  val clientStatsStrategyJMX = JMX.register(new ClientStatisticsRequestStrategyMBeanImpl(clientName, serviceName, clientStatsStrategy))
  val serverErrorStrategyJMX = JMX.register(new ServerErrorStrategyMBeanImpl(clientName, serviceName, serverErrorStrategy))

  val strategy = CompositeCanServeRequestStrategy(clientStatsStrategy, serverErrorStrategy)

  private val statsJMX = JMX.register(new NetworkClientStatisticsMBeanImpl(clientName, serviceName, stats))

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val request = e.getMessage.asInstanceOf[Request[_, _]]
    log.debug("Writing request: %s".format(request))

    requestMap.put(request.id, request)
    stats.beginRequest(request.node, request.id)

    val message = NorbertProtos.NorbertMessage.newBuilder
    message.setRequestIdMsb(request.id.getMostSignificantBits)
    message.setRequestIdLsb(request.id.getLeastSignificantBits)
    message.setMessageName(request.name)
    message.setMessage(ProtoUtils.byteArrayToByteString(request.requestBytes, avoidByteStringCopy))

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

        stats.endRequest(request.node, request.id)

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

trait HealthScoreCalculator extends Logging {
  def doCalculation[T](p: Map[T, StatsEntry], f: Map[T, StatsEntry]): Double = {
    def fSize = f.values.map(_.size).sum
    def pSize = p.values.map(_.size).sum

    val fTotal = f.map{ case(k, v) => v.percentile * v.size }.sum
    val pTotal = p.map{ case(k, v) => v.percentile * v.size }.sum

    val result = safeDivide(fTotal + pTotal, fSize + pSize)(0)

    if(result < 0.0) {
      log.warn("Found a negative result when calculating weighted median. Pending = %s. Finished = %s. fSize = %s. pSize = %s. fTotal = %s. pTotal = %s"
                .format(p, f, fSize, pSize, fTotal, pTotal))
    }
    result
  }

  def averagePercentiles[T](s: Map[T, StatsEntry]): Double = {
    val size = s.values.map(_.size).sum
    val total = s.map { case (k, v) => v.percentile * v.size }.sum
    safeDivide(total, size)(0.0)
  }
}

class ClientStatisticsRequestStrategy(val stats: CachedNetworkStatistics[Node, UUID],
                                      @volatile var outlierMultiplier: Double,
                                      @volatile var outlierConstant: Double,
                                      clock: Clock)
  extends CanServeRequestStrategy with Logging with HealthScoreCalculator {
  // Must be more than outlierMultiplier * average + outlierConstant ms the others by default

  val totalNodesMarkedDown = new AtomicInteger(0)

  val canServeRequests = CacheMaintainer(clock, 200L, () => {
    val s = stats.getStatistics(0.5)
    val (p, f) = (s.map(_.pending).getOrElse(Map.empty), s.map(_.finished).getOrElse(Map.empty))

    val clusterMedian = doCalculation(p, f)

    f.map { case (n, nodeN) =>
      val nodeP = p.get(n).getOrElse(StatsEntry(0.0, 0, 0))

      val nodeMedian = doCalculation(Map(0 -> nodeP),Map(0 -> nodeN))
      val available = nodeMedian <= clusterMedian * outlierMultiplier + outlierConstant

      if(!available) {
        log.warn("Node %s has a median response time of %f. The cluster response time is %f. Routing requests away temporarily.".format(n, nodeMedian, clusterMedian))
        totalNodesMarkedDown.incrementAndGet
      }
      (n, available)
    }
  })

  def canServeRequest(node: Node) = {
    val map = canServeRequests.get
    map.flatMap(_.get(node)).getOrElse(true)
  }
}

trait ClientStatisticsRequestStrategyMBean extends CanServeRequestStrategyMBean {
  def getOutlierMultiplier: Double
  def getOutlierConstant: Double

  def setOutlierMultiplier(m: Double)
  def setOutlierConstant(c: Double)

  def getTotalNodesMarkedDown: Int
}

class ClientStatisticsRequestStrategyMBeanImpl(clientName: Option[String], serviceName: String, strategy: ClientStatisticsRequestStrategy)
  extends MBean(classOf[ClientStatisticsRequestStrategyMBean], JMX.name(clientName, serviceName))
  with ClientStatisticsRequestStrategyMBean {

  def getCanServeRequests = toJMap(strategy.canServeRequests.get.getOrElse(Map.empty).map { case (n, a) => (n.id -> a) })

  def getOutlierMultiplier = strategy.outlierMultiplier

  def getOutlierConstant = strategy.outlierConstant

  def setOutlierMultiplier(m: Double) { strategy.outlierMultiplier = m}

  def setOutlierConstant(c: Double) = { strategy.outlierConstant = c}

  def getTotalNodesMarkedDown = strategy.totalNodesMarkedDown.get.abs
}