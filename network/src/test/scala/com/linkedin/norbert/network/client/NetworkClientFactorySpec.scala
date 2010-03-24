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
package com.linkedin.norbert.network.client

import loadbalancer.LoadBalancerFactoryComponent
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.google.protobuf.Message
import com.linkedin.norbert.cluster._
import com.linkedin.norbert.network.{NoNodesAvailableException, NetworkNotStartedException}
import com.linkedin.norbert.network.common.ClusterIoClientComponent

class NetworkClientFactorySpec extends SpecificationWithJUnit with Mockito {
  val clusterClient = mock[ClusterClient]
  val networkClientFactory = new NetworkClientFactory with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent {
    val lb = mock[LoadBalancer]
    val clusterIoClient = mock[ClusterIoClient]
    val loadBalancerFactory = mock[LoadBalancerFactory]
    val clusterClient = NetworkClientFactorySpec.this.clusterClient
  }
  val nodes = List(Node(1, "", true), Node(2, "", true), Node(3, "", true))

  "NetworkClientFactory" should {
    "throw NetworkNotStartedException if a method is called before start" in {
      val ncf = new NetworkClientFactory with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent {
        val clusterIoClient = null
        val clusterClient = null
        val loadBalancerFactory = null
      }
      ncf.newNetworkClient must throwA[NetworkNotStartedException]
    }

    "throw a ClusterShutdownException if the cluster has been shut down" in {
      val ncf = new NetworkClientFactory with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent {
        val clusterIoClient = mock[ClusterIoClient]
        val clusterClient = mock[ClusterClient]
        val loadBalancerFactory = mock[LoadBalancerFactory]
      }
      ncf.shutdown
      ncf.start must throwA[ClusterShutdownException]
      ncf.newNetworkClient must throwA[ClusterShutdownException]
    }

    "start the cluster, creates a load balancer and register itself as a listener when started" in {
      doNothing.when(clusterClient).start
      clusterClient.addListener(any[ClusterListener]) returns ClusterListenerKey(1)
      clusterClient.nodes returns nodes
      networkClientFactory.loadBalancerFactory.newLoadBalancer(nodes) returns networkClientFactory.lb

      networkClientFactory.start

      clusterClient.start was called
      clusterClient.addListener(any[ClusterListener]) was called
      clusterClient.nodes was called
      networkClientFactory.loadBalancerFactory.newLoadBalancer(nodes) was called
    }

    "update the load balancer when a Connected or NodesChanged event is received" in {
      var listener: ClusterListener = null
      clusterClient.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
      clusterClient.nodes returns nodes
      networkClientFactory.loadBalancerFactory.newLoadBalancer(nodes) returns networkClientFactory.lb

      networkClientFactory.start

      listener must notBeNull
      listener.handleClusterEvent(ClusterEvents.Connected(nodes))
      listener.handleClusterEvent(ClusterEvents.NodesChanged(nodes))
      networkClientFactory.loadBalancerFactory.newLoadBalancer(nodes) was called.times(3)
    }

    "shut down the clusterIoClient when shutdown is called" in {
      doNothing.when(networkClientFactory.clusterIoClient).shutdown

      networkClientFactory.shutdown

      networkClientFactory.clusterIoClient.shutdown was called
    }
  }

  "NetworkClient" should {
    "throw ClusterDisconnectedException if the cluster is disconnected when a method is called" in {
      networkClientFactory.start
      val nc = networkClientFactory.newNetworkClient
      val message = mock[Message]
      val node = Node(1, "", true)

      nc.broadcastMessage(message) must throwA[ClusterDisconnectedException]
      nc.sendMessageToNode(message, node) must throwA[ClusterDisconnectedException]
      nc.sendMessage(message) must throwA[ClusterDisconnectedException]
    }

    "throw ClusterShutdownException if the cluster is shut down when a method is called" in {
      networkClientFactory.start
      val nc = networkClientFactory.newNetworkClient
      networkClientFactory.shutdown
      val message = mock[Message]
      val node = Node(1, "", true)

      nc.broadcastMessage(message) must throwA[ClusterShutdownException]
      nc.sendMessageToNode(message, node) must throwA[ClusterShutdownException]
      nc.sendMessage(message) must throwA[ClusterShutdownException]
    }

    "send a message to every available node for broadcastMessage" in {
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      val message = mock[Message]
//      nodes.foreach(n => doNothing.when(clusterIoClient).sendMessage(n, message, null))

      networkClientFactory.start
      networkClientFactory.newNetworkClient.broadcastMessage(message) must notBeNull

//      nodes.foreach(n => clusterIoClient.sendMessage(n, message, null) was called)
    }

    "send message to the specified node in sendMessageToNode" in {
      val message = mock[Message]
      val node = Node(1, "", true)
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClientFactory.start
      networkClientFactory.newNetworkClient.sendMessageToNode(message, node) must notBeNull

//      clusterIoClient.sendMessage(node, message, null) was called
    }

    "throw an InvalidNodeException if the node provided to sendMessageToNode is not currently availabe" in {
      val message = mock[Message]
      val node = Node(4, "", true)
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClientFactory.start
      networkClientFactory.newNetworkClient.sendMessageToNode(message, node) must throwA[InvalidNodeException]

//      clusterIoClient.sendMessage(node, message, null) wasnt called
    }

    "send the provide message to the node specified by the load balancer for sendMessage" in {
      val message = mock[Message]
      val node = Node(1, "", true)
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      networkClientFactory.loadBalancerFactory.newLoadBalancer(nodes) returns networkClientFactory.lb
      networkClientFactory.lb.nextNode returns Some(node)
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClientFactory.start
      networkClientFactory.newNetworkClient.sendMessage(message) must notBeNull

      networkClientFactory.lb.nextNode was called
//      clusterIoClient.sendMessage(node, message, null) was called
    }

    "throw InvalidClusterException if there is no load balancer instance when sendMessage is called" in {
      val message = mock[Message]
      val node = Node(1, "", true)
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      networkClientFactory.loadBalancerFactory.newLoadBalancer(nodes) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClientFactory.start
      networkClientFactory.newNetworkClient.sendMessage(message) must throwA[InvalidClusterException]

//      clusterIoClient.sendMessage(node, message, null) wasnt called
    }

    "throw NoSuchNodeException if load balancer returns None when sendMessage is called" in {
      val message = mock[Message]
      val node = Node(1, "", true)
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      networkClientFactory.loadBalancerFactory.newLoadBalancer(nodes) returns networkClientFactory.lb
      networkClientFactory.lb.nextNode returns None
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClientFactory.start
      networkClientFactory.newNetworkClient.sendMessage(message) must throwA[NoNodesAvailableException]

      networkClientFactory.lb.nextNode was called
//      clusterIoClient.sendMessage(node, message, null) wasnt called
    }
  }
}
