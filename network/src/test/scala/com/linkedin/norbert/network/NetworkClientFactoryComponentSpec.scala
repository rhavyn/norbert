/*
 * Copyright 2009 LinkedIn, Inc
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
package com.linkedin.norbert.network

import java.net.InetSocketAddress
import netty.RequestHandlerComponent
import com.google.protobuf.Message
import org.specs.SpecificationWithJUnit
import com.linkedin.norbert.cluster._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.NorbertException

class NetworkClientFactoryComponentSpec extends SpecificationWithJUnit with Mockito with NetworkClientFactoryComponent
        with ChannelPoolComponent with BootstrapFactoryComponent with ClusterComponent with ZooKeeperMonitorComponent
        with ClusterWatcherComponent with RouterFactoryComponent with ClusterManagerComponent with RequestHandlerComponent
        with MessageRegistryComponent {

  type Id = Int
  val clusterWatcher = null
  val zooKeeperMonitor = null
  val clusterManager = null
  val routerFactory = null
  val cluster = mock[Cluster]
  val bootstrapFactory = mock[BootstrapFactory]
  val networkClientFactory = mock[NetworkClientFactory]
  val channelPool = mock[ChannelPool]
  val messageRegistry = null

  "NetworkClientFactory" should {
    "when calling newNetworkClient" in {
      "wait for the cluster to be available" in {
        doNothing.when(cluster).awaitConnectionUninterruptibly

        new NetworkClientFactory().newNetworkClient

        cluster.awaitConnectionUninterruptibly was called
      }

      "looks up the current route" in {
        cluster.router returns mock[Option[Router]]

        new NetworkClientFactory().newNetworkClient

        cluster.router was called
      }

      "add the new network client as a listener to the cluster" in {
        doNothing.when(cluster).addListener(isA(classOf[ClusterListener]))

        new NetworkClientFactory().newNetworkClient

        cluster.addListener(isA(classOf[ClusterListener])) was called
      }
    }

    "shutdown should shutdown the network and cluster" in {
      doNothing.when(channelPool).shutdown
      doNothing.when(cluster).shutdown

      new NetworkClientFactory().shutdown

      channelPool.shutdown was called
      cluster.shutdown was called
    }
  }

  "NetworkClient" should {
    "when sendMessage is called" in {
      "throw a ClusterDisconnectedException if the cluster is disconnected" in {
        cluster.router returns None

        new NetworkClientFactory().newNetworkClient.sendMessage(Array(1, 2), mock[Message]) must throwA[ClusterDisconnectedException]
      }

      "look up the route for the ids provided" in {
        val router = mock[Router]
        List(1, 2).foreach(router(_) returns Some(Node(1, new InetSocketAddress(13131), Array(0), true)))

        val client = new NetworkClientFactory().newNetworkClient
        client.asInstanceOf[ClusterListener].handleClusterEvent(ClusterEvents.Connected(Array[Node](), Some(router)))
        client.sendMessage(Array(1, 2), NorbertProtos.Ping.newBuilder.setTimestamp(1L).build)

        List(1, 2).foreach(router(_) was called)
      }

      "sends the message" in {
        val router = mock[Router]
        val nodes = Array(Node(1, new InetSocketAddress(13131), Array(0), true), Node(2, new InetSocketAddress(13132), Array(1), true))
        List(0, 1).foreach { i =>
          router(i + 1) returns Some(nodes(i))
        }
        doNothing.when(channelPool).sendRequest(containAll(nodes), isA(classOf[Request]))

        val client = new NetworkClientFactory().newNetworkClient
        client.asInstanceOf[ClusterListener].handleClusterEvent(ClusterEvents.Connected(Array[Node](), Some(router)))
        client.sendMessage(Array(1, 2), NorbertProtos.Ping.newBuilder.setTimestamp(1L).build)

        channelPool.sendRequest(containAll(nodes), isA(classOf[Request])) was called
      }
    }

    "when sendMessage is called with a message customizer" in {
      "throw a ClusterDisconnectedException if the cluster is disconnected" in {
        cluster.router returns None

        new NetworkClientFactory().newNetworkClient.sendMessage(Array(1, 2), mock[Message], messageCustomizer _) must throwA[ClusterDisconnectedException]
      }

      "look up the route for the ids provided" in {
        val router = mock[Router]
        List(1, 2).foreach(router(_) returns Some(Node(1, new InetSocketAddress(13131), Array(0), true)))

        val client = new NetworkClientFactory().newNetworkClient
        client.asInstanceOf[ClusterListener].handleClusterEvent(ClusterEvents.Connected(Array[Node](), Some(router)))
        client.sendMessage(Array(1, 2), NorbertProtos.Ping.newBuilder.setTimestamp(1L).build, messageCustomizer _)

        List(1, 2).foreach(router(_) was called)
      }

      "sends the message" in {
        val router = mock[Router]
        val nodes = Array(Node(1, new InetSocketAddress(13131), Array(0), true), Node(2, new InetSocketAddress(13132), Array(1), true))
        List(0, 1).foreach(i => router(i + 1) returns Some(nodes(i)))

        List(0, 1).foreach(i => doNothing.when(channelPool).sendRequest(containAll(Array(nodes(i))), isA(classOf[Request])))

        val client = new NetworkClientFactory().newNetworkClient
        client.asInstanceOf[ClusterListener].handleClusterEvent(ClusterEvents.Connected(Array[Node](), Some(router)))
        client.sendMessage(Array(1, 2), NorbertProtos.Ping.newBuilder.setTimestamp(1L).build, messageCustomizer _)

        List(0, 1).foreach(i => channelPool.sendRequest(containAll(Array(nodes(i))), isA(classOf[Request])) was called)
      }

      "calls the message customizer" in {
        var callCount = 0
        var nodeMap = Map[Node, Seq[Int]]()

        def mc(message: Message, node: Node, ids: Seq[Int]) = {
          callCount += 1
          nodeMap = nodeMap + (node -> ids)
          message
        }

        val router = mock[Router]
        val nodes = Array(Node(1, new InetSocketAddress(13131), Array(0), true), Node(2, new InetSocketAddress(13132), Array(1), true))
        List(1, 2).foreach(i => router(i) returns Some(nodes(0)))
        List(3, 4).foreach(i => router(i) returns Some(nodes(1)))

        List(0, 1).foreach(i => doNothing.when(channelPool).sendRequest(containAll(Array(nodes(i))), isA(classOf[Request])))

        val client = new NetworkClientFactory().newNetworkClient
        client.asInstanceOf[ClusterListener].handleClusterEvent(ClusterEvents.Connected(Array[Node](), Some(router)))
        client.sendMessage(Array(1, 2, 3, 4), NorbertProtos.Ping.newBuilder.setTimestamp(1L).build, mc _)

        callCount must be_==(2)
        nodeMap.size must be_==(2)
        nodeMap(nodes(0)) must haveTheSameElementsAs(Array(1, 2))
        nodeMap(nodes(1)) must haveTheSameElementsAs(Array(3, 4))
      }
    }

    "when sendMessage is called with a response gatherer" in {
      "it calls the response gatherer" in {
        var callCount = 0
        def rg(message: Message, ri: ResponseIterator) = {
          callCount += 1
          123454321
        }

        val router = mock[Router]
        val nodes = Array(Node(1, new InetSocketAddress(13131), Array(0), true), Node(2, new InetSocketAddress(13132), Array(1), true))
        List(0, 1).foreach(i => router(i + 1) returns Some(nodes(i)))

        List(0, 1).foreach(i => doNothing.when(channelPool).sendRequest(containAll(Array(nodes(i))), isA(classOf[Request])))

        val client = new NetworkClientFactory().newNetworkClient
        client.asInstanceOf[ClusterListener].handleClusterEvent(ClusterEvents.Connected(Array[Node](), Some(router)))
        val ping = NorbertProtos.Ping.newBuilder.setTimestamp(1L).build
        client.sendMessage(Array(1, 2), ping, rg _) must be_==(123454321)

        callCount must be_==(1)
      }

      "it rethrows exceptions thrown by the response gatherer" in {
        def rg(message: Message, ri: ResponseIterator): Message = {
          throw new NorbertException("RG Exception")
        }

        val router = mock[Router]
        val nodes = Array(Node(1, new InetSocketAddress(13131), Array(0), true), Node(2, new InetSocketAddress(13132), Array(1), true))
        List(0, 1).foreach(i => router(i + 1) returns Some(nodes(i)))

        List(0, 1).foreach(i => doNothing.when(channelPool).sendRequest(containAll(Array(nodes(i))), isA(classOf[Request])))

        val client = new NetworkClientFactory().newNetworkClient
        client.asInstanceOf[ClusterListener].handleClusterEvent(ClusterEvents.Connected(Array[Node](), Some(router)))
        client.sendMessage(Array(1, 2), NorbertProtos.Ping.newBuilder.setTimestamp(1L).build, rg _) must throwA[NorbertException]
      }
    }

    "when sentMessageToNode is called" in {
      "a message is sent to the Node" in {
        val node = Node(1, new InetSocketAddress(13131), Array(0), true)
        doNothing.when(channelPool).sendRequest(containAll(List(node)), isA(classOf[Request]))

        val client = new NetworkClientFactory().newNetworkClient
        client.sendMessageToNode(node, NorbertProtos.Ping.newBuilder.setTimestamp(1L).build)

        channelPool.sendRequest(containAll(List(node)), isA(classOf[Request])) was called
      }

      "an InvalidNodeException is thrown if the node is not available" in {
        val node = Node(1, new InetSocketAddress(13131), Array(0), false)

        val client = new NetworkClientFactory().newNetworkClient
        client.sendMessageToNode(node, NorbertProtos.Ping.newBuilder.setTimestamp(1L).build) must throwA[InvalidNodeException]
      }
    }

    "when isConnected is called" in {
      "return true if it has a valid router" in {
        cluster.router returns Some(mock[Router])

        new NetworkClientFactory().newNetworkClient.isConnected must beTrue
      }

      "return false if does not have a valid router" in {
        cluster.router returns None

        new NetworkClientFactory().newNetworkClient.isConnected must beFalse
      }
    }

    "when close is called" in {
      "remove itself as a cluster listener" in {
        val client = new NetworkClientFactory().newNetworkClient

        doNothing.when(cluster).removeListener(client.asInstanceOf[ClusterListener])

        client.close

        cluster.removeListener(client.asInstanceOf[ClusterListener]) was called
      }
    }
  }

  def messageCustomizer(message: Message, node: Node, isd: Seq[Int]) = message  
}
