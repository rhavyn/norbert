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

import java.net.InetSocketAddress
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.{ChannelPipelineFactory, Channel}
import org.mockito.Matchers._
import org.specs.mock.Mockito
import org.specs.SpecificationWithJUnit
import com.linkedin.norbert.cluster._
import com.linkedin.norbert.network._

class NettyNetworkServerComponentSpec extends SpecificationWithJUnit with Mockito with NettyNetworkServerComponent
        with BootstrapFactoryComponent with ClusterComponent with ZooKeeperMonitorComponent
        with ClusterWatcherComponent with RouterFactoryComponent with ClusterManagerComponent
        with NettyRequestHandlerComponent with MessageRegistryComponent with NetworkClientFactoryComponent
        with ClusterIoClientComponent with ResponseHandlerComponent with MessageExecutorComponent {

  val clusterWatcher = null
  val zooKeeperMonitor = null
  val clusterManager = null
  val routerFactory = null
  val requestHandler = mock[NettyRequestHandler]
  val messageRegistry = null
  val clusterIoClient = null
  val responseHandler = null
  val messageExecutor = mock[MessageExecutor]
  val networkClientFactory = mock[NetworkClientFactory]
  val cluster = mock[Cluster]
  val bootstrapFactory = mock[BootstrapFactory]
  val networkServer = mock[NetworkServer]

  "NettyNetworkServer" should {
    "when instantiated with a node id" in {
      "connect to the cluster and listen to cluster events" in {
        doNothing.when(cluster).start
        doNothing.when(cluster).awaitConnectionUninterruptibly
        doNothing.when(cluster).addListener(isA(classOf[ClusterListener]))
        bootstrapFactory.newServerBootstrap returns mock[ServerBootstrap]
        cluster.nodeWithId(1) returns Some(Node(1, "localhost", 31313, Array(0, 1, 2), true))

        val server = new NettyNetworkServer(1)
        server.bind

        cluster.start was called
        cluster.awaitConnectionUninterruptibly was called
        cluster.addListener(isA(classOf[ClusterListener])) was called
      }

      "look up the specified node and bind to that node's address" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        cluster.nodeWithId(1) returns Some(Node(1, "localhost", 31313, Array(0, 1, 2), true))
        bootstrap.bind(new InetSocketAddress("localhost", 31313)) returns mock[Channel]

        val server = new NettyNetworkServer(1)
        server.bind

        cluster.nodeWithId(1) was called
        bootstrap.bind(new InetSocketAddress("localhost", 31313)) was called
      }

      "correctly configures the ServerBootstrap" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        doNothing.when(bootstrap).setPipelineFactory(isA(classOf[ChannelPipelineFactory]))
        doNothing.when(bootstrap).setOption("child.tcpNoDelay", true)
        doNothing.when(bootstrap).setOption("child.reuseAddress", true)
        doNothing.when(bootstrap).setOption("tcpNoDelay", true)
        doNothing.when(bootstrap).setOption("reuseAddress", true)

        val server = new NettyNetworkServer(1)

        bootstrap.setPipelineFactory(isA(classOf[ChannelPipelineFactory])) was called
        bootstrap.setOption("child.tcpNoDelay", true) was called
        bootstrap.setOption("child.reuseAddress", true) was called
        bootstrap.setOption("tcpNoDelay", true) was called
        bootstrap.setOption("reuseAddress", true) was called
      }

      "throws InvalidNodeException if the specified node id doesn't exist" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        doNothing.when(cluster).awaitConnectionUninterruptibly
        cluster.nodeWithId(1) returns None

        val server = new NettyNetworkServer(1)
        server.bind must throwA[InvalidNodeException]

        bootstrap.bind(isA(classOf[InetSocketAddress])) wasnt called
      }

      "shutdown properly shuts down the cluster, messageExecutor, requestHandler and network client factory" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        doNothing.when(cluster).awaitConnectionUninterruptibly
        cluster.nodeWithId(1) returns None
        doNothing.when(cluster).shutdown
        doNothing.when(networkClientFactory).shutdown
        doNothing.when(messageExecutor).shutdown
        doNothing.when(requestHandler).shutdown

        new NettyNetworkServer(1).shutdown

        cluster.shutdown was called
        networkClientFactory.shutdown was called
        messageExecutor.shutdown was called
        requestHandler.shutdown was called
      }

      "makes the node available via currentNode" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        val node = Node(1, "localhost", 31313, Array(0, 1, 2), true)
        cluster.nodeWithId(1) returns Some(node)
        bootstrap.bind(new InetSocketAddress("localhost", 31313)) returns mock[Channel]

        val server = new NettyNetworkServer(1)
        server.bind

        server.currentNode must be(node)
      }

      "marks the node available in the cluster" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        val node = Node(1, "localhost", 31313, Array(0, 1, 2), true)
        cluster.nodeWithId(1) returns Some(node)
        doNothing.when(cluster).markNodeAvailable(1)
        
        bootstrap.bind(new InetSocketAddress("localhost", 31313)) returns mock[Channel]

        val server = new NettyNetworkServer(1)
        server.bind
        server.handleClusterEvent(ClusterEvents.Connected(null, null))

        cluster.markNodeAvailable(1) was called
      }

      "doesn't mark the node available in the cluster" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        val node = Node(1, "localhost", 31313, Array(0, 1, 2), true)
        cluster.nodeWithId(1) returns Some(node)
        doNothing.when(cluster).markNodeAvailable(1)

        bootstrap.bind(new InetSocketAddress("localhost", 31313)) returns mock[Channel]

        val server = new NettyNetworkServer(1)
        server.bind(false)
        server.handleClusterEvent(ClusterEvents.Connected(null, null))

        cluster.markNodeAvailable(1) wasnt called        
      }

      "marks the node available after calling markNodeAvailable" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        val node = Node(1, "localhost", 31313, Array(0, 1, 2), true)
        cluster.nodeWithId(1) returns Some(node)
        doNothing.when(cluster).markNodeAvailable(1)

        bootstrap.bind(new InetSocketAddress("localhost", 31313)) returns mock[Channel]

        val server = new NettyNetworkServer(1)
        server.bind(false)
        server.markAvailable
        server.handleClusterEvent(ClusterEvents.Connected(null, null))

        cluster.markNodeAvailable(1) was called.twice         
      }

      "throw a NetworkingException if markAvailable is called before calling bind" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap

        val server = new NettyNetworkServer(1)
        server.markAvailable must throwA[NetworkingException]
      }
    }

    "when instantiated with an InetSocketAddress" in {
      val isa = new InetSocketAddress(13131)

      "connect to the cluster and listen to cluster events" in {
        doNothing.when(cluster).start
        doNothing.when(cluster).awaitConnectionUninterruptibly
        doNothing.when(cluster).addListener(isA(classOf[ClusterListener]))
        bootstrapFactory.newServerBootstrap returns mock[ServerBootstrap]
        cluster.nodeWithAddress(isa) returns Some(Node(1, "some.host.com", 31313, Array(0, 1, 2), true))

        val server = new NettyNetworkServer(isa)
        server.bind

        cluster.start was called
        cluster.awaitConnectionUninterruptibly was called
        cluster.addListener(isA(classOf[ClusterListener])) was called
      }

      "look up the specified node and bind to the specified address" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        cluster.nodeWithAddress(isa) returns Some(Node(1, "some.host.com", 31313, Array(0, 1, 2), true))
        bootstrap.bind(isa) returns mock[Channel]

        val server = new NettyNetworkServer(isa)
        server.bind

        cluster.nodeWithAddress(isa) was called
        bootstrap.bind(isa) was called
      }

      "correctly configures the ServerBootstrap" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        doNothing.when(bootstrap).setPipelineFactory(isA(classOf[ChannelPipelineFactory]))
        doNothing.when(bootstrap).setOption("child.tcpNoDelay", true)
        doNothing.when(bootstrap).setOption("child.reuseAddress", true)
        doNothing.when(bootstrap).setOption("tcpNoDelay", true)
        doNothing.when(bootstrap).setOption("reuseAddress", true)

        val server = new NettyNetworkServer(isa)

        bootstrap.setPipelineFactory(isA(classOf[ChannelPipelineFactory])) was called
        bootstrap.setOption("child.tcpNoDelay", true) was called
        bootstrap.setOption("child.reuseAddress", true) was called
        bootstrap.setOption("tcpNoDelay", true) was called
        bootstrap.setOption("reuseAddress", true) was called
      }

      "throws InvalidNodeException if the specified address doesn't exist" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        doNothing.when(cluster).awaitConnectionUninterruptibly
        cluster.nodeWithAddress(isa) returns None

        val server = new NettyNetworkServer(isa)
        server.bind must throwA[InvalidNodeException]

        bootstrap.bind(isa) wasnt called
      }

      "makes the node available via currentNode" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        val node = Node(1, "some.host.com", 31313, Array(0, 1, 2), true)
        cluster.nodeWithAddress(isa) returns Some(node)
        bootstrap.bind(isa) returns mock[Channel]

        val server = new NettyNetworkServer(isa)
        server.bind

        server.currentNode must be(node)
      }

      "marks the node available in the cluster" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        val node = Node(1, "localhost", 31313, Array(0, 1, 2), true)
        cluster.nodeWithAddress(isa) returns Some(Node(1, "some.host.com", 31313, Array(0, 1, 2), true))
        doNothing.when(cluster).markNodeAvailable(1)

        bootstrap.bind(new InetSocketAddress("localhost", 31313)) returns mock[Channel]

        val server = new NettyNetworkServer(isa)
        server.bind
        server.handleClusterEvent(ClusterEvents.Connected(null, null))

        cluster.markNodeAvailable(1) was called
      }

      "doesn't mark the node available in the cluster" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        val node = Node(1, "localhost", 31313, Array(0, 1, 2), true)
        cluster.nodeWithAddress(isa) returns Some(Node(1, "some.host.com", 31313, Array(0, 1, 2), true))
        doNothing.when(cluster).markNodeAvailable(1)

        bootstrap.bind(new InetSocketAddress("localhost", 31313)) returns mock[Channel]

        val server = new NettyNetworkServer(isa)
        server.bind(false)
        server.handleClusterEvent(ClusterEvents.Connected(null, null))

        cluster.markNodeAvailable(1) wasnt called
      }

      "marks the node available after calling markNodeAvailable" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap
        val node = Node(1, "localhost", 31313, Array(0, 1, 2), true)
        cluster.nodeWithAddress(isa) returns Some(Node(1, "some.host.com", 31313, Array(0, 1, 2), true))
        doNothing.when(cluster).markNodeAvailable(1)

        bootstrap.bind(new InetSocketAddress("localhost", 31313)) returns mock[Channel]

        val server = new NettyNetworkServer(isa)
        server.bind(false)
        server.markAvailable
        server.handleClusterEvent(ClusterEvents.Connected(null, null))

        cluster.markNodeAvailable(1) was called.twice
      }

      "throw a NetworkingException if markAvailable is called before calling bind" in {
        val bootstrap = mock[ServerBootstrap]
        bootstrapFactory.newServerBootstrap returns bootstrap

        val server = new NettyNetworkServer(isa)
        server.markAvailable must throwA[NetworkingException]
      }
    }
  }
}
