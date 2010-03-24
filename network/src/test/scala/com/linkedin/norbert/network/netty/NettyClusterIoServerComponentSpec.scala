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

class NettyClusterIoServerComponentSpec extends SpecificationWithJUnit with Mockito with NettyClusterIoServerComponent {
  val bootstrap = mock[ServerBootstrap]
  val clusterIoServer = new NettyClusterIoServer(bootstrap)

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

    "shutdown should shutdown an opened socket" in {
      val channel = mock[Channel]
      val future = mock[ChannelFuture]
      channel.close returns future
      future.awaitUninterruptibly returns future
      bootstrap.bind(any[InetSocketAddress]) returns channel

      clusterIoServer.bind(Node(1, "localhost:31313", false), true)
      clusterIoServer.shutdown

      channel.close was called
      future.awaitUninterruptibly was called
    }
  }
}
