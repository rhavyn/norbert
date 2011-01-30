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

import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentHashMap}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import common.{ClusterIoClientComponent}
import cluster.Node
import logging.Logging

/**
 * A <code>ClusterIoClientComponent</code> implementation that uses Netty for network communication.
 */
trait NettyClusterIoClientComponent extends ClusterIoClientComponent {

  class NettyClusterIoClient(channelPoolFactory: ChannelPoolFactory) extends ClusterIoClient with UrlParser with Logging {
    private val channelPools = new ConcurrentHashMap[Node, ChannelPool]


    def sendMessage[RequestMsg, ResponseMsg](node: Node, request: Request[RequestMsg, ResponseMsg]) {
      if (node == null || request == null) throw new NullPointerException

      var pool = channelPools.get(node)
      if (pool == null) {
        val (address, port) = parseUrl(node.url)

        pool = channelPoolFactory.newChannelPool(new InetSocketAddress(address, port))
        channelPools.putIfAbsent(node, pool)
        pool = channelPools.get(node)
      }

      try {
        pool.sendRequest(request)
      } catch {
        case ex: ChannelPoolClosedException =>
          // ChannelPool was closed, try again
          sendMessage(node, request)
      }
    }

    def nodesChanged(nodes: Set[Node]) = {
      import scala.collection.JavaConversions._
      channelPools.keySet.foreach { node =>
        if (!nodes.contains(node)) {
          val pool = channelPools.remove(node)
          pool.close
          log.debug("Closing pool for unavailable node: %s".format(node))
        }
      }
    }

    def shutdown = {
      import scala.collection.JavaConversions._

      channelPools.keySet.foreach { key =>
        channelPools.get(key) match {
          case null => // do nothing
          case pool =>
            pool.close
            channelPools.remove(key)
        }
      }

      channelPoolFactory.shutdown

      log.debug("NettyClusterIoClient shut down")
    }
  }

  private class NorbertChannelPipelineFactory extends ChannelPipelineFactory {
    val p = Channels.pipeline

    def getPipeline = p
  }
}
