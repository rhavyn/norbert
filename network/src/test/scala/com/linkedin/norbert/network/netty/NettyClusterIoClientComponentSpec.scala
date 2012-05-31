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

import org.specs.Specification
import java.net.InetSocketAddress
import org.specs.util.WaitFor
import org.specs.mock.Mockito
import cluster.{InvalidNodeException, Node}
import common.AlwaysAvailableRequestStrategy

class NettyClusterIoClientComponentSpec extends Specification with Mockito with WaitFor with NettyClusterIoClientComponent {
  val messageRegistry = null

  val channelPoolFactory = mock[ChannelPoolFactory]
  val channelPool = mock[ChannelPool]

  val clusterIoClient = new NettyClusterIoClient(channelPoolFactory, AlwaysAvailableRequestStrategy)

  val node = Node(1, "localhost:31313", true)
  val address = new InetSocketAddress("localhost", 31313)

  "NettyClusterIoClient" should {
    "create a new ChannelPool if no pool is available" in {
      doNothing.when(channelPool).sendRequest(any[Request[_, _]])
      channelPoolFactory.newChannelPool(address) returns channelPool



      clusterIoClient.sendMessage(node, mock[Request[_, _]])

      got {
        one(channelPool).sendRequest(any[Request[_, _]])
        one(channelPoolFactory).newChannelPool(address)
      }
    }

    "not create a ChannelPool if a pool is available" in {
      channelPoolFactory.newChannelPool(address) returns channelPool
      doNothing.when(channelPool).sendRequest(any[Request[_, _]])

      clusterIoClient.sendMessage(node, mock[Request[_, _]])
      clusterIoClient.sendMessage(node, mock[Request[_, _]])

      got {
        two(channelPool).sendRequest(any[Request[_, _]])
        one(channelPoolFactory).newChannelPool(address)
      }
    }

    "close an open ChannelPool if the Node is no longer available" in {
      doNothing.when(channelPool).sendRequest(any[Request[_, _]])
      doNothing.when(channelPool).close
      channelPoolFactory.newChannelPool(address) returns channelPool

      clusterIoClient.sendMessage(node, mock[Request[_, _]])
      clusterIoClient.nodesChanged(Set())

      got {
        one(channelPoolFactory).newChannelPool(address)
        one(channelPool).close
      }
    }

    "throw an InvalidNodeException if a Node with an invalid url is provided" in {
      channelPoolFactory.newChannelPool(address) returns channelPool
      clusterIoClient.sendMessage(Node(1, "foo", true), mock[Request[_, _]]) must throwA[InvalidNodeException]
      clusterIoClient.sendMessage(Node(1, "foo:foo", true), mock[Request[_, _]]) must throwA[InvalidNodeException]
    }

    "close all ChannelPools when shutdown is called" in {
      channelPoolFactory.newChannelPool(address) returns channelPool
      doNothing.when(channelPool).close
      doNothing.when(channelPoolFactory).shutdown

      clusterIoClient.sendMessage(node, mock[Request[_, _]])
      clusterIoClient.shutdown

      got {
        one(channelPool).close
        one(channelPoolFactory).shutdown
      }
    }
  }
}
