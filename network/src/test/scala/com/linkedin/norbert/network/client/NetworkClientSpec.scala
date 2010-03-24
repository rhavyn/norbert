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

class NetworkClientSpec extends SpecificationWithJUnit with Mockito {
  val clusterClient = mock[ClusterClient]
  val networkClient = new NetworkClient with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent {
    val lb = mock[LoadBalancer]
    val clusterIoClient = mock[ClusterIoClient]
    val loadBalancerFactory = mock[LoadBalancerFactory]
    val clusterClient = NetworkClientSpec.this.clusterClient
  }

  val nodes = List(Node(1, "", true), Node(2, "", true), Node(3, "", true))
  val message = mock[Message]

  "NetworkClient" should {
    "update the load balancer when a Connected or NodesChanged event is received" in {
      val cc = mock[ClusterClient]
      doNothing.when(clusterClient).start
      cc.addListener(any[ClusterListener]) returns ClusterListenerKey(1)
      cc.nodes returns nodes
      var listener: ClusterListener = null
      cc.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
      cc.nodes returns nodes

      val nc = new NetworkClient with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent {
        val lb = mock[LoadBalancer]
        val clusterIoClient = mock[ClusterIoClient]
        val loadBalancerFactory = mock[LoadBalancerFactory]
        val clusterClient = cc
      }

      nc.loadBalancerFactory.newLoadBalancer(nodes) returns nc.lb

      nc.start

      listener must notBeNull
      listener.handleClusterEvent(ClusterEvents.Connected(nodes))
      listener.handleClusterEvent(ClusterEvents.NodesChanged(nodes))
      nc.loadBalancerFactory.newLoadBalancer(nodes) was called.times(3)
    }


    "start the cluster, creates a load balancer and register itself as a listener when started" in {
      val cc = mock[ClusterClient]
      doNothing.when(clusterClient).start
      cc.addListener(any[ClusterListener]) returns ClusterListenerKey(1)
      cc.nodes returns nodes

      val nc = new NetworkClient with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent {
        val lb = mock[LoadBalancer]
        val clusterIoClient = mock[ClusterIoClient]
        val loadBalancerFactory = mock[LoadBalancerFactory]
        val clusterClient = cc
      }

      nc.loadBalancerFactory.newLoadBalancer(nodes) returns nc.lb

      nc.start
      
      cc.start was called
      cc.addListener(any[ClusterListener]) was called
      cc.nodes was called
      nc.loadBalancerFactory.newLoadBalancer(nodes) was called
    }

    "shut down the clusterIoClient when shutdown is called" in {
      doNothing.when(networkClient.clusterIoClient).shutdown

      networkClient.shutdown

      networkClient.clusterIoClient.shutdown was called
    }

    "throw ClusterDisconnectedException if the cluster is disconnected when a method is called" in {
      networkClient.start

      networkClient.broadcastMessage(message) must throwA[ClusterDisconnectedException]
      networkClient.sendMessageToNode(message, nodes(1)) must throwA[ClusterDisconnectedException]
      networkClient.sendMessage(message) must throwA[ClusterDisconnectedException]
    }

    "throw ClusterShutdownException if the cluster is shut down when a method is called" in {
      networkClient.shutdown

      networkClient.broadcastMessage(message) must throwA[ClusterShutdownException]
      networkClient.sendMessageToNode(message, nodes(1)) must throwA[ClusterShutdownException]
      networkClient.sendMessage(message) must throwA[ClusterShutdownException]
    }

    "send a message to every available node for broadcastMessage" in {
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
//      nodes.foreach(n => doNothing.when(clusterIoClient).sendMessage(n, message, null))

      networkClient.start
      networkClient.broadcastMessage(message) must notBeNull

//      nodes.foreach(n => clusterIoClient.sendMessage(n, message, null) was called)
    }

    "send message to the specified node in sendMessageToNode" in {
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessageToNode(message, nodes(1)) must notBeNull

//      clusterIoClient.sendMessage(node, message, null) was called
    }

    "throw an InvalidNodeException if the node provided to sendMessageToNode is not currently availabe" in {
      val node = Node(4, "", true)
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessageToNode(message, node) must throwA[InvalidNodeException]

//      clusterIoClient.sendMessage(node, message, null) wasnt called
    }

    "send the provided message to the node specified by the load balancer for sendMessage" in {
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
      networkClient.lb.nextNode returns Some(nodes(1))
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessage(message) must notBeNull

      networkClient.lb.nextNode was called
//      clusterIoClient.sendMessage(node, message, null) was called
    }

    "throw InvalidClusterException if there is no load balancer instance when sendMessage is called" in {
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      networkClient.loadBalancerFactory.newLoadBalancer(nodes) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessage(message) must throwA[InvalidClusterException]

//      clusterIoClient.sendMessage(node, message, null) wasnt called
    }

    "throw NoSuchNodeException if load balancer returns None when sendMessage is called" in {
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
      networkClient.lb.nextNode returns None
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessage(message) must throwA[NoNodesAvailableException]

      networkClient.lb.nextNode was called
//      clusterIoClient.sendMessage(node, message, null) wasnt called
    }
  }
}
