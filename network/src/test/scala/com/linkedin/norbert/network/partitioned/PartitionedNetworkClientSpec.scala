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
package partitioned

import common.{ClusterIoClientComponent, BaseNetworkClientSpecification}
import loadbalancer.{PartitionedLoadBalancerFactory, PartitionedLoadBalancer, PartitionedLoadBalancerFactoryComponent}
import java.util.concurrent.ExecutionException
import cluster.{Node, InvalidClusterException, ClusterDisconnectedException, ClusterClientComponent}

class PartitionedNetworkClientSpec extends BaseNetworkClientSpecification {
  val networkClient = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent
      with PartitionedLoadBalancerFactoryComponent[Int] {
    val lb = mock[PartitionedLoadBalancer[Int]]
    val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
    val clusterIoClient = mock[ClusterIoClient]
    val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
  }

//  networkClient.messageRegistry.contains(any[Message]) returns true

  "PartitionedNetworkClient" should {
    "provide common functionality" in { sharedFunctionality }

    "throw ClusterDisconnectedException if the cluster is disconnected when a method is called" in {
      networkClient.start

      networkClient.broadcastMessage(request) must throwA[ClusterDisconnectedException]
      networkClient.sendRequestToNode(request, nodes(1)) must throwA[ClusterDisconnectedException]
      networkClient.sendRequest(1, request) must throwA[ClusterDisconnectedException]
      networkClient.sendRequest(Set(1, 2), request) must throwA[ClusterDisconnectedException]
    }

    "throw ClusterShutdownException if the cluster is shut down when a method is called" in {
      networkClient.shutdown

      networkClient.broadcastMessage(request) must throwA[NetworkShutdownException]
      networkClient.sendRequestToNode(request, nodes(1)) must throwA[NetworkShutdownException]
      networkClient.sendRequest(1, request) must throwA[NetworkShutdownException]
      networkClient.sendRequest(Set(1, 2), request) must throwA[NetworkShutdownException]
    }

//    "throw an InvalidMessageException if an unregistered message is sent" in {
//      clusterClient.nodes returns nodeSet
//      clusterClient.isConnected returns true
////      networkClient.messageRegistry.contains(any[Message]) returns false
//
//      networkClient.start
//
//      networkClient.broadcastMessage(request) must throwA[InvalidMessageException]
//      networkClient.sendRequestToNode(request, nodes(1)) must throwA[InvalidMessageException]
//      networkClient.sendRequest(1, request) must throwA[InvalidMessageException]
//      networkClient.sendRequest(Set(1, 2), request) must throwA[InvalidMessageException]
//    }

    "when sendRequest(id, message) is called" in {
      "send the provided message to the node specified by the load balancer for sendRequest" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode(1) returns Some(nodes(1))
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(1, request) must notBeNull

        there was one(networkClient.lb).nextNode(1)
//      clusterIoClient.sendRequest(node, message, null) was called
      }

      "throw InvalidClusterException if there is no load balancer instance when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(1, request) must throwA[InvalidClusterException]

//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode(1) returns None
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(1, request) must throwA[NoNodesAvailableException]

        there was one(networkClient.lb).nextNode(1)
//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }
    }

    "when sendRequest(ids, message) is called" in {
      "send the provided message to the node specified by the load balancer" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode(1) returns Some(nodes(1))
        networkClient.lb.nextNode(2) returns Some(nodes(2))
        networkClient.lb.nextNode(3) returns Some(nodes(1))

//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), request) must notBeNull

        got {
          one(networkClient.lb).nextNode(1)
          one(networkClient.lb).nextNode(2)
          one(networkClient.lb).nextNode(3)
        }
//      clusterIoClient.sendRequest(node, message, null) was called
      }

      "throw InvalidClusterException if there is no load balancer instance when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), request) must throwA[InvalidClusterException]

//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode(1) returns None
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), request) must throwA[NoNodesAvailableException]

        there was one(networkClient.lb).nextNode(1)
//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }
    }

    "when sendRequest(ids, message, messageCustomizer) is called" in {
      "send the provided message to the node specified by the load balancer" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        List(1, 2, 3).foreach(networkClient.lb.nextNode(_) returns Some(Node(1, "localhost:31313", true)))
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _)

        List(1, 2, 3).foreach(there was one(networkClient.lb).nextNode(_))
//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "call the message customizer" in {
        var callCount = 0
        var nodeMap = Map[Node, Set[Int]]()

        def mc(node: Node, ids: Set[Int]): Ping = {
          callCount += 1
          nodeMap = nodeMap + (node -> ids)
          new Ping
        }

        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        List(1, 2).foreach(networkClient.lb.nextNode(_) returns Some(nodes(0)))
        List(3, 4).foreach(networkClient.lb.nextNode(_) returns Some(nodes(1)))

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3, 4), mc _)

        callCount must be_==(2)
        nodeMap.size must be_==(2)
        nodeMap(nodes(0)) must haveTheSameElementsAs(Array(1, 2))
        nodeMap(nodes(1)) must haveTheSameElementsAs(Array(3, 4))
      }

      "treats an exception from the message customizer as a failed response" in {
        def mc(node: Node, ids: Set[Int]): Ping = {
          throw new Exception
        }

        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        List(1, 2).foreach(networkClient.lb.nextNode(_) returns Some(nodes(0)))

        networkClient.start
        val ri = networkClient.sendRequest(Set(1, 2), mc _)
        ri.hasNext must beTrue
        ri.next must throwA[ExecutionException]
      }

      "throw InvalidClusterException if there is no load balancer instance when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _)  must throwA[InvalidClusterException]

//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode(1) returns None
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _) must throwA[NoNodesAvailableException]

        there was one(networkClient.lb).nextNode(1)
//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }
    }

    "when sendRequest is called with a response aggregator" in {
      "it calls the response aggregator" in {
        var callCount = 0
        def ag(ri: ResponseIterator[Ping]) = {
          callCount += 1
          123454321
        }

        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        List(1, 2).foreach(networkClient.lb.nextNode(_) returns Some(nodes(0)))

        networkClient.start
        networkClient.sendRequest(Set(1, 2), (node: Node, ids: Set[Int]) => request, ag _) must be_==(123454321)

        callCount must be_==(1)
      }

      "it rethrows exceptions thrown by the response aggregator" in {
        def ag(ri: ResponseIterator[Ping]): Int = {
          throw new Exception
        }

        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        List(1, 2).foreach(networkClient.lb.nextNode(_) returns Some(nodes(0)))

        networkClient.start
        networkClient.sendRequest(Set(1, 2), (node: Node, ids: Set[Int]) => request, ag _) must throwA[Exception]
      }
    }
  }

  def messageCustomizer(node: Node, ids: Set[Int]): Ping = new Ping
}
