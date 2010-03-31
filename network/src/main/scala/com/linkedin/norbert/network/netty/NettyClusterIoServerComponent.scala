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

import com.linkedin.norbert.network.server.ClusterIoServerComponent
import com.linkedin.norbert.cluster.Node
import org.jboss.netty.bootstrap.ServerBootstrap
import java.net.InetSocketAddress
import org.jboss.netty.channel.{ChannelException, Channel}
import com.linkedin.norbert.network.NetworkingException
import com.linkedin.norbert.logging.Logging
import org.jboss.netty.channel.group.ChannelGroup

trait NettyClusterIoServerComponent extends ClusterIoServerComponent {
  class NettyClusterIoServer(bootstrap: ServerBootstrap, channelGroup: ChannelGroup) extends ClusterIoServer with UrlParser with Logging {
    private var serverChannel: Channel = _

    def bind(node: Node, wildcard: Boolean) = {
      val (_, port) = parseUrl(node.url)
      try {
        val address = new InetSocketAddress(port)
        log.ifDebug("Binding server socket to %s", address)
        serverChannel = bootstrap.bind(address)
      } catch {
        case ex: ChannelException => throw new NetworkingException("Unable to bind to %s".format(node), ex)
      }
    }

    def shutdown = if (serverChannel != null) {
      serverChannel.close.awaitUninterruptibly
      channelGroup.close.awaitUninterruptibly
      bootstrap.releaseExternalResources
    }
  }
}
