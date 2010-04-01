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

import com.google.protobuf.Message
import com.linkedin.norbert.cluster.Node
import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentHashMap}
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.common.{MessageRegistryComponent, ClusterIoClientComponent}
import org.jboss.netty.channel.{Channels, ChannelFactory, ChannelPipelineFactory}

/**
 * A <code>ClusterIoClientComponent</code> implementation that uses Netty for network communication.
 */
trait NettyClusterIoClientComponent extends ClusterIoClientComponent {
  this: MessageRegistryComponent =>

  class NettyClusterIoClient(channelPoolFactory: ChannelPoolFactory) extends ClusterIoClient with UrlParser with Logging {
    private val channelPools = new ConcurrentHashMap[Node, ChannelPool]

    def sendMessage(node: Node, message: Message, responseCallback: (Either[Throwable, Message]) => Unit) = {
      if (node == null || message == null || responseCallback == null) throw new NullPointerException

      var pool = channelPools.get(node)
      if (pool == null) {
        val (address, port) = parseUrl(node.url)

        pool = channelPoolFactory.newChannelPool(new InetSocketAddress(address, port))
        channelPools.putIfAbsent(node, pool)
        pool = channelPools.get(node)
      }

      try {
        pool.sendRequest(Request(message, responseCallback))
      } catch {
        case ex: ChannelPoolClosedException =>
          // ChannelPool was closed, try again
          sendMessage(node, message, responseCallback)
      }
    }

    def nodesChanged(nodes: Seq[Node]) = {
      val nodeSet = Set(nodes: _*)

      import scala.collection.jcl.Conversions._
      channelPools.keySet.foreach { node =>
        if (!nodeSet.contains(node)) {
          val pool = channelPools.remove(node)
          pool.close
          log.ifDebug("Closing pool for unavailable node: %s", node)
        }
      }
    }

    def shutdown = {
      import scala.collection.jcl.Conversions._

      channelPools.keySet.foreach { key =>
        channelPools.get(key) match {
          case null => // do nothing
          case pool =>
            pool.close
            channelPools.remove(key)
        }
      }

      channelPoolFactory.shutdown

      log.ifDebug("NettyClusterIoClient shut down")
    }
  }

  protected implicit def defaultBootstrapFactory(channelFactory: ChannelFactory, remoteAddress: InetSocketAddress, connectTimeoutMillis: Int) = {
  }

  private class NorbertChannelPipelineFactory extends ChannelPipelineFactory {
    val p = Channels.pipeline

    def getPipeline = p
  }
}
