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

import loadbalancer.{LoadBalancerFactory, LoadBalancer, LoadBalancerFactoryComponent}
import com.google.protobuf.Message
import com.linkedin.norbert.cluster._
import com.linkedin.norbert.network.common.{BaseNetworkClientSpecification, MessageRegistry, MessageRegistryComponent, ClusterIoClientComponent}
import com.linkedin.norbert.network.{NetworkShutdownException, InvalidMessageException, NoNodesAvailableException}

class NetworkClientSpec extends BaseNetworkClientSpecification {
  val networkClient = new NetworkClient with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent with MessageRegistryComponent {
    val lb = mock[LoadBalancer]
    val loadBalancerFactory = mock[LoadBalancerFactory]
    val clusterIoClient = mock[ClusterIoClient]
    val messageRegistry = mock[MessageRegistry]
    val clusterClient = NetworkClientSpec.this.clusterClient
  }

  networkClient.messageRegistry.contains(any[Message]) returns true

  "NetworkClient" should {
    "provide common functionality" in { sharedFunctionality }

    "throw ClusterDisconnectedException if the cluster is disconnected when a method is called" in {
      networkClient.start

      networkClient.broadcastMessage(message) must throwA[ClusterDisconnectedException]
      networkClient.sendMessageToNode(message, nodes(1)) must throwA[ClusterDisconnectedException]
      networkClient.sendMessage(message) must throwA[ClusterDisconnectedException]
    }

    "throw ClusterShutdownException if the cluster is shut down when a method is called" in {
      networkClient.shutdown

      networkClient.broadcastMessage(message) must throwA[NetworkShutdownException]
      networkClient.sendMessageToNode(message, nodes(1)) must throwA[NetworkShutdownException]
      networkClient.sendMessage(message) must throwA[NetworkShutdownException]
    }

    "throw an InvalidMessageException if an unregistered message is sent" in {
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      networkClient.messageRegistry.contains(any[Message]) returns false

      networkClient.start

      networkClient.broadcastMessage(message) must throwA[InvalidMessageException]
      networkClient.sendMessageToNode(message, nodes(1)) must throwA[InvalidMessageException]
      networkClient.sendMessage(message) must throwA[InvalidMessageException]
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
