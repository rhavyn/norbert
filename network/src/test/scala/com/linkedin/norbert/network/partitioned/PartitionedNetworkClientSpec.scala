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
package com.linkedin.norbert.network.partitioned

import loadbalancer.{PartitionedLoadBalancerFactory, PartitionedLoadBalancer, PartitionedLoadBalancerFactoryComponent}
import com.linkedin.norbert.network.common.{MessageRegistry, ClusterIoClientComponent, MessageRegistryComponent, BaseNetworkClientSpecification}
import com.google.protobuf.Message
import com.linkedin.norbert.cluster._
import java.net.InetSocketAddress
import com.linkedin.norbert.protos.NorbertProtos
import java.util.concurrent.ExecutionException
import com.linkedin.norbert.network.{ResponseIterator, NoNodesAvailableException, InvalidMessageException}

class PartitionedNetworkClientSpec extends BaseNetworkClientSpecification {
  val networkClient = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent
      with PartitionedLoadBalancerFactoryComponent[Int] {
    val lb = mock[PartitionedLoadBalancer[Int]]
    val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
    val clusterIoClient = mock[ClusterIoClient]
    val messageRegistry = mock[MessageRegistry]
    val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
  }

  networkClient.messageRegistry.contains(any[Message]) returns true

  "PartitionedNetworkClient" should {
    "provide common functionality" in { sharedFunctionality }

    "throw ClusterDisconnectedException if the cluster is disconnected when a method is called" in {
      networkClient.start

      networkClient.broadcastMessage(message) must throwA[ClusterDisconnectedException]
      networkClient.sendMessageToNode(message, nodes(1)) must throwA[ClusterDisconnectedException]
      networkClient.sendMessage(1, message) must throwA[ClusterDisconnectedException]
      networkClient.sendMessage(Array(1, 2), message) must throwA[ClusterDisconnectedException]
    }

    "throw ClusterShutdownException if the cluster is shut down when a method is called" in {
      networkClient.shutdown

      networkClient.broadcastMessage(message) must throwA[ClusterShutdownException]
      networkClient.sendMessageToNode(message, nodes(1)) must throwA[ClusterShutdownException]
      networkClient.sendMessage(1, message) must throwA[ClusterShutdownException]
      networkClient.sendMessage(Array(1, 2), message) must throwA[ClusterShutdownException]
    }

    "throw an InvalidMessageException if an unregistered message is sent" in {
      clusterClient.nodes returns nodes
      clusterClient.isConnected returns true
      networkClient.messageRegistry.contains(any[Message]) returns false

      networkClient.start

      networkClient.broadcastMessage(message) must throwA[InvalidMessageException]
      networkClient.sendMessageToNode(message, nodes(1)) must throwA[InvalidMessageException]
      networkClient.sendMessage(1, message) must throwA[InvalidMessageException]
      networkClient.sendMessage(Array(1, 2), message) must throwA[InvalidMessageException]
    }

    "when sendMessage(id, message) is called" in {
      "send the provided message to the node specified by the load balancer for sendMessage" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        networkClient.lb.nextNode(1) returns Some(nodes(1))
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(1, message) must notBeNull

        networkClient.lb.nextNode(1) was called
//      clusterIoClient.sendMessage(node, message, null) was called
      }

      "throw InvalidClusterException if there is no load balancer instance when sendMessage is called" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(1, message) must throwA[InvalidClusterException]

//      clusterIoClient.sendMessage(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendMessage is called" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        networkClient.lb.nextNode(1) returns None
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(1, message) must throwA[NoNodesAvailableException]

        networkClient.lb.nextNode(1) was called
//      clusterIoClient.sendMessage(node, message, null) wasnt called
      }
    }

    "when sendMessage(ids, message) is called" in {
      "send the provided message to the node specified by the load balancer" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        networkClient.lb.nextNode(1) returns Some(nodes(1))
        networkClient.lb.nextNode(2) returns Some(nodes(2))
        networkClient.lb.nextNode(3) returns Some(nodes(1))

//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(Array(1, 2, 3), message) must notBeNull

        networkClient.lb.nextNode(1) was called
        networkClient.lb.nextNode(2) was called
        networkClient.lb.nextNode(3) was called
//      clusterIoClient.sendMessage(node, message, null) was called
      }

      "throw InvalidClusterException if there is no load balancer instance when sendMessage is called" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(Array(1, 2, 3), message) must throwA[InvalidClusterException]

//      clusterIoClient.sendMessage(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendMessage is called" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        networkClient.lb.nextNode(1) returns None
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(Array(1, 2, 3), message) must throwA[NoNodesAvailableException]

        networkClient.lb.nextNode(1) was called
//      clusterIoClient.sendMessage(node, message, null) wasnt called
      }
    }

    "when sendMessage(ids, message, messageCustomizer) is called" in {
      "send the provided message to the node specified by the load balancer" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        List(1, 2, 3).foreach(networkClient.lb.nextNode(_) returns Some(Node(1, "localhost:31313", true)))
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(Array(1, 2, 3), message, messageCustomizer _)

        List(1, 2, 3).foreach(networkClient.lb.nextNode(_) was called)
//      clusterIoClient.sendMessage(node, message, null) wasnt called
      }

      "call the message customizer" in {
        var callCount = 0
        var nodeMap = Map[Node, Seq[Int]]()

        def mc(message: Message, node: Node, ids: Seq[Int]) = {
          callCount += 1
          nodeMap = nodeMap + (node -> ids)
          message
        }

        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        List(1, 2).foreach(networkClient.lb.nextNode(_) returns Some(nodes(0)))
        List(3, 4).foreach(networkClient.lb.nextNode(_) returns Some(nodes(1)))

        networkClient.start
        networkClient.sendMessage(Array(1, 2, 3, 4), message, mc _)

        callCount must be_==(2)
        nodeMap.size must be_==(2)
        nodeMap(nodes(0)) must haveTheSameElementsAs(Array(1, 2))
        nodeMap(nodes(1)) must haveTheSameElementsAs(Array(3, 4))
      }

      "treats an exception from the message customizer as a failed response" in {
        def mc(message: Message, node: Node, ids: Seq[Int]) = {
          throw new Exception
        }

        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        List(1, 2).foreach(networkClient.lb.nextNode(_) returns Some(nodes(0)))

        networkClient.start
        val ri = networkClient.sendMessage(Array(1, 2), message, mc _)
        ri.hasNext must beTrue
        ri.next must throwA[ExecutionException]
      }

      "throw InvalidClusterException if there is no load balancer instance when sendMessage is called" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(Array(1, 2, 3), message, messageCustomizer _)  must throwA[InvalidClusterException]

//      clusterIoClient.sendMessage(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendMessage is called" in {
        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        networkClient.lb.nextNode(1) returns None
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

        networkClient.start
        networkClient.sendMessage(Array(1, 2, 3), message, messageCustomizer _) must throwA[NoNodesAvailableException]

        networkClient.lb.nextNode(1) was called
//      clusterIoClient.sendMessage(node, message, null) wasnt called
      }
    }

    "when sendMessage is called with a response aggregator" in {
      "it calls the response aggregator" in {
        var callCount = 0
        def ag(message: Message, ri: ResponseIterator) = {
          callCount += 1
          123454321
        }

        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        List(1, 2).foreach(networkClient.lb.nextNode(_) returns Some(nodes(0)))

        networkClient.start
        networkClient.sendMessage(Array(1, 2), message, ag _) must be_==(123454321)

        callCount must be_==(1)
      }

      "it rethrows exceptions thrown by the response aggregator" in {
        def ag(message: Message, ri: ResponseIterator): Int = {
          throw new Exception
        }

        clusterClient.nodes returns nodes
        clusterClient.isConnected returns true
        networkClient.loadBalancerFactory.newLoadBalancer(nodes) returns networkClient.lb
        List(1, 2).foreach(networkClient.lb.nextNode(_) returns Some(nodes(0)))

        networkClient.start
        networkClient.sendMessage(Array(1, 2), message, ag _) must throwA[Exception]
      }
    }
  }

  def messageCustomizer(message: Message, node: Node, isd: Seq[Int]) = message
}
