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

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.jboss.netty.bootstrap.ServerBootstrap
import com.linkedin.norbert.cluster.{InvalidNodeException, Node}
import java.net.InetSocketAddress
import org.jboss.netty.channel.{ChannelFuture, Channel}
import org.jboss.netty.channel.group.{ChannelGroupFuture, ChannelGroup}

class NettyClusterIoServerComponentSpec extends SpecificationWithJUnit with Mockito with NettyClusterIoServerComponent {
  val bootstrap = mock[ServerBootstrap]
  val channelGroup = mock[ChannelGroup]
  val clusterIoServer = new NettyClusterIoServer(bootstrap, channelGroup)

  "NettyClusterIoServer" should {
    "bind should fail if the node's url is in the incorrect format" in {
      clusterIoServer.bind(Node(1, "", false), true) must throwA[InvalidNodeException]
      clusterIoServer.bind(Node(1, "localhost", false), true) must throwA[InvalidNodeException]
      clusterIoServer.bind(Node(1, "localhost:foo", false), true) must throwA[InvalidNodeException]
    }

    "binds to the specified port" in {
      val address = new InetSocketAddress(31313)
      val channel = mock[Channel]
      bootstrap.bind(address) returns channel

      clusterIoServer.bind(Node(1, "localhost:31313", false), true)

      bootstrap.bind(address) was called
    }

    "shutdown should shutdown opened sockets" in {
      val channel = mock[Channel]
      val socketFuture = mock[ChannelFuture]
      val groupFuture = mock[ChannelGroupFuture]
      channel.close returns socketFuture
      channelGroup.close returns groupFuture
      socketFuture.awaitUninterruptibly returns socketFuture
      groupFuture.awaitUninterruptibly returns groupFuture
      bootstrap.bind(any[InetSocketAddress]) returns channel


      clusterIoServer.bind(Node(1, "localhost:31313", false), true)
      clusterIoServer.shutdown

      channel.close was called
      socketFuture.awaitUninterruptibly was called

      channelGroup.close was called
      groupFuture.awaitUninterruptibly was called
    }
  }
}
