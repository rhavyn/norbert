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

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.jboss.netty.channel.{ChannelFutureListener, ChannelFuture, Channel}
import java.util.concurrent.{TimeoutException, ArrayBlockingQueue, LinkedBlockingQueue}
import java.net.InetSocketAddress
import jmx.JMX.MBean
import jmx.JMX
import logging.Logging
import cluster.{Node, ClusterClient}
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean, AtomicInteger}
import norbertutils.{SystemClock}
import java.io.IOException
import common.{BackoffStrategy, SimpleBackoffStrategy}

class ChannelPoolClosedException extends Exception

class ChannelPoolFactory(maxConnections: Int, writeTimeoutMillis: Int, bootstrap: ClientBootstrap, errorStrategy: Option[BackoffStrategy]) {

  def newChannelPool(address: InetSocketAddress): ChannelPool = {
    val group = new DefaultChannelGroup("norbert-client [%s]".format(address))
    new ChannelPool(address, maxConnections, writeTimeoutMillis, bootstrap, group, errorStrategy)
  }

  def shutdown: Unit = {
    bootstrap.releaseExternalResources
  }
}

class ChannelPool(address: InetSocketAddress, maxConnections: Int, writeTimeoutMillis: Int, bootstrap: ClientBootstrap,
    channelGroup: ChannelGroup, val errorStrategy: Option[BackoffStrategy]) extends Logging {
  private val pool = new ArrayBlockingQueue[Channel](maxConnections)
  private val waitingWrites = new LinkedBlockingQueue[Request[_, _]]
  private val poolSize = new AtomicInteger(0)
  private val closed = new AtomicBoolean
  private val requestsSent = new AtomicInteger(0)

  private val jmxHandle = JMX.register(new MBean(classOf[ChannelPoolMBean], "address=%s,port=%d".format(address.getHostName, address.getPort)) with ChannelPoolMBean {
    def getWriteQueueSize = waitingWrites.size

    def getOpenChannels = poolSize.get

    def getMaxChannels = maxConnections

    def getNumberRequestsSent = requestsSent.get.abs
  })

  def sendRequest[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg]): Unit = if (closed.get) {
    throw new ChannelPoolClosedException
  } else {
    checkoutChannel match {
      case Some(channel) =>
        writeRequestToChannel(request, channel)
        checkinChannel(channel)

      case None =>
        waitingWrites.offer(request)
        openChannel(request)
    }
  }

  def close {
    if (closed.compareAndSet(false, true)) {
      jmxHandle.foreach { JMX.unregister(_) }
      channelGroup.close.awaitUninterruptibly
    }
  }

  private def checkinChannel(channel: Channel) {
    while (!waitingWrites.isEmpty) {
      waitingWrites.poll match {
        case null => // do nothing

        case request =>
          if((System.currentTimeMillis - request.timestamp) < writeTimeoutMillis) writeRequestToChannel(request, channel)
          else request.onFailure(new TimeoutException("Timed out while waiting to write"))
      }
    }

    pool.offer(channel)
  }

  private def checkoutChannel: Option[Channel] = {
    var found = false
    var channel: Channel = null

    while (!pool.isEmpty && !found) {
      pool.poll match {
        case null => // do nothing

        case c =>
          if (c.isConnected) {
            channel = c
            found = true
          } else {
            poolSize.decrementAndGet
          }
      }
    }

    if (channel == null) None else Some(channel)
  }

  private def openChannel(request: Request[_, _]) {
    if (poolSize.incrementAndGet > maxConnections) {
      poolSize.decrementAndGet
      log.debug("Unable to open channel, pool is full")
    } else {
      log.debug("Opening a channel to: %s".format(address))

      bootstrap.connect(address).addListener(new ChannelFutureListener {
        def operationComplete(openFuture: ChannelFuture) = {
          if (openFuture.isSuccess) {
            val channel = openFuture.getChannel
            channelGroup.add(channel)
            checkinChannel(channel)
          } else {
            log.error(openFuture.getCause, "Error when opening channel to: %s, marking offline".format(address))
            errorStrategy.foreach(_.notifyFailure(request.node))
            poolSize.decrementAndGet

            request.onFailure(openFuture.getCause)
          }
        }
      })
    }
  }

  private def writeRequestToChannel(request: Request[_, _], channel: Channel) {
    log.debug("Writing to %s: %s".format(channel, request))
    requestsSent.incrementAndGet
    channel.write(request).addListener(new ChannelFutureListener {
      def operationComplete(writeFuture: ChannelFuture) = if (!writeFuture.isSuccess) {
        // Take the node out of rotation for a bit
        log.error("IO exception for " + request.node + ", marking node offline")
        errorStrategy.foreach(_.notifyFailure(request.node))
        channel.close

        request.onFailure(writeFuture.getCause)
      }
    })
  }

  def numRequestsSent = requestsSent.get
}

trait ChannelPoolMBean {
  def getOpenChannels: Int
  def getMaxChannels: Int
  def getWriteQueueSize: Int
  def getNumberRequestsSent: Int
}
