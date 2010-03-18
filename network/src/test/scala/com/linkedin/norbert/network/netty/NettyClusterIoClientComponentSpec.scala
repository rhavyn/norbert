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
import org.jboss.netty.bootstrap.ClientBootstrap
import java.net.InetSocketAddress
import org.jboss.netty.channel.ChannelFactory
import org.specs.util.WaitFor
import org.specs.mock.Mockito
import com.google.protobuf.Message
import com.linkedin.norbert.cluster.{InvalidNodeException, Node}

class NettyClusterIoClientComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with NettyClusterIoClientComponent {
  val bootstrap = mock[ClientBootstrap]
  var bootstrapFactoryWasCalled = false
  var bootstrapAddress: InetSocketAddress = _
  def bootstrapFactory(channelFactory: ChannelFactory, remoteAddress: InetSocketAddress, connectTimeoutMillis: Int) = {
    bootstrapFactoryWasCalled = true
    bootstrapAddress = remoteAddress
    bootstrap
  }

  val channelPoolFactory = mock[ChannelPoolFactory]
  val channelPool = mock[ChannelPool]

  val clusterIoClient = new NettyClusterIoClient(1, mock[ChannelFactory], channelPoolFactory)(bootstrapFactory _)

  val node = Node(1, "localhost:31313", true)
  val address = new InetSocketAddress("localhost", 31313)

  "NettyClusterIoClient" should {
    "create a new ChannelPool if no pool is available" in {
      doNothing.when(channelPool).sendRequest(any[Request])
      channelPoolFactory.newChannelPool(bootstrap) returns channelPool

      clusterIoClient.sendMessage(node, mock[Message], e => null)

      channelPool.sendRequest(any[Request]) was called
      bootstrapFactoryWasCalled must beTrue
      bootstrapAddress must be_==(address)
      channelPoolFactory.newChannelPool(bootstrap) was called
    }

    "not create a ChannelPool if a pool is available" in {
      channelPoolFactory.newChannelPool(bootstrap) returns channelPool
      doNothing.when(channelPool).sendRequest(any[Request])

      clusterIoClient.sendMessage(node, mock[Message], e => null)

      bootstrapFactoryWasCalled must beTrue
      bootstrapFactoryWasCalled = false

      clusterIoClient.sendMessage(node, mock[Message], e => null)
      channelPool.sendRequest(any[Request]) was called.twice
      bootstrapFactoryWasCalled must beFalse
      channelPoolFactory.newChannelPool(bootstrap) was called.once
    }

    "throw an InvalidNodeException if a Node with an invalid url is provided" in {
      channelPoolFactory.newChannelPool(bootstrap) returns channelPool
      clusterIoClient.sendMessage(Node(1, "foo", true), mock[Message], e => null) must throwA[InvalidNodeException]
      clusterIoClient.sendMessage(Node(1, "foo:foo", true), mock[Message], e => null) must throwA[InvalidNodeException]
    }

    "close all ChannelPools when shutdown is called" in {
      channelPoolFactory.newChannelPool(bootstrap) returns channelPool
      doNothing.when(channelPool).close

      clusterIoClient.sendMessage(node, mock[Message], e => null)
      clusterIoClient.shutdown

      channelPool.close was called
    }
  }
}
