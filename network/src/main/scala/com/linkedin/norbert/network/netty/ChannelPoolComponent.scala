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

import java.util.concurrent.{ArrayBlockingQueue, LinkedBlockingQueue}
import com.linkedin.norbert.network.Request
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.jboss.netty.channel.{ChannelFutureListener, ChannelFuture, Channel}
import com.linkedin.norbert.util.Logging

trait ChannelPoolComponent {
  class ChannelPoolClosedException extends Exception

  class ChannelPool private[netty] (maxConnections: Int, writeTimeout: Int, bootstrap: ClientBootstrap, channelGroup: ChannelGroup) extends Logging {
    def this(maxConnections: Int, writeTimeout: Int, bootstrap: ClientBootstrap) = this(maxConnections,
      writeTimeout, bootstrap, new DefaultChannelGroup("norbert-client [%s]".format(bootstrap.getOption("remoteAddress"))))
    
    private val pool = new ArrayBlockingQueue[Channel](maxConnections)
    private val waitingWrites = new LinkedBlockingQueue[Request]
    private val poolSize = new AtomicInteger(0)
    private val closed = new AtomicBoolean

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
        channelGroup.close.awaitUninterruptibly        
      }
    }

    private def checkinChannel(channel: Channel) {
      while (!waitingWrites.isEmpty) {
        waitingWrites.poll match {
          case null => // do nothing
          case request => if((System.currentTimeMillis - request.timestamp) < writeTimeout) writeRequestToChannel(request, channel)
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
        log.ifDebug("Unable to open channel, pool is full")
      } else {
        log.ifDebug("Opening a channel to: %s", bootstrap.getOption("remoteAddress"))

        bootstrap.connect.addListener(new ChannelFutureListener {
          def operationComplete(openFuture: ChannelFuture) = {
            if (openFuture.isSuccess) {
              val channel = openFuture.getChannel
              channelGroup.add(channel)
              checkinChannel(channel)
            } else {
              log.error(openFuture.getCause, "Error when opening channel to: %s", bootstrap.getOption("remoteAddress"))
              poolSize.decrementAndGet
            }
          }
        })
      }
    }

    private def writeRequestToChannel(request: Request, channel: Channel) {
      log.ifDebug("Writing to %s: %s", channel, request)
      channel.write(request).addListener(new ChannelFutureListener {
        def operationComplete(writeFuture: ChannelFuture) = if (!writeFuture.isSuccess) {
          request.offerResponse(Left(writeFuture.getCause))
        }
      })
    }
  }
}
