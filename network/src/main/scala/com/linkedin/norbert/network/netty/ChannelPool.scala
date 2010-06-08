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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.jboss.netty.channel.{ChannelFutureListener, ChannelFuture, Channel}
import java.util.concurrent.{TimeoutException, ArrayBlockingQueue, LinkedBlockingQueue}
import java.net.InetSocketAddress
import jmx.JMX.MBean
import jmx.JMX
import logging.Logging

class ChannelPoolClosedException extends Exception

class ChannelPoolFactory(maxConnections: Int, writeTimeoutMillis: Int, bootstrap: ClientBootstrap) {
  def newChannelPool(address: InetSocketAddress): ChannelPool = {
    val group = new DefaultChannelGroup("norbert-client [%s]".format(address))
    new ChannelPool(address, maxConnections, writeTimeoutMillis, bootstrap, group)
  }

  def shutdown: Unit = bootstrap.releaseExternalResources
}

class ChannelPool(address: InetSocketAddress, maxConnections: Int, writeTimeoutMillis: Int, bootstrap: ClientBootstrap,
    channelGroup: ChannelGroup) extends Logging {
  private val pool = new ArrayBlockingQueue[Channel](maxConnections)
  private val waitingWrites = new LinkedBlockingQueue[Request]
  private val poolSize = new AtomicInteger(0)
  private val closed = new AtomicBoolean
  private val requestsSent = new AtomicInteger(0)

  private val jmxHandle = JMX.register(new MBean(classOf[ChannelPoolMBean], "address=%s,port=%d".format(address.getHostName, address.getPort)) with ChannelPoolMBean {
    def getWriteQueueSize = waitingWrites.size

    def getOpenChannels = poolSize.get

    def getMaxChannels = maxConnections

    def getNumberRequestsSent = requestsSent.get
  })

  def sendRequest(request: Request): Unit = if (closed.get) {
    throw new ChannelPoolClosedException
  } else {
    checkoutChannel match {
      case Some(channel) =>
        writeRequestToChannel(request, channel)
        checkinChannel(channel)

      case None =>
        openChannel
        waitingWrites.offer(request)
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
          else request.responseCallback(Left(new TimeoutException("Timed out while waiting to write")))
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

  private def openChannel {
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
            log.error(openFuture.getCause, "Error when opening channel to: %s".format(address))
            poolSize.decrementAndGet
          }
        }
      })
    }
  }

  private def writeRequestToChannel(request: Request, channel: Channel) {
    log.debug("Writing to %s: %s".format(channel, request))
    requestsSent.incrementAndGet
    channel.write(request).addListener(new ChannelFutureListener {
      def operationComplete(writeFuture: ChannelFuture) = if (!writeFuture.isSuccess) {
        request.responseCallback(Left(writeFuture.getCause))
      }
    })
  }
}

trait ChannelPoolMBean {
  def getOpenChannels: Int
  def getMaxChannels: Int
  def getWriteQueueSize: Int
  def getNumberRequestsSent: Int
}
