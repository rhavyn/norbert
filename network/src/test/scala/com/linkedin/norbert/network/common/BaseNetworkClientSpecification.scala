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
package common

import com.google.protobuf.Message
import org.specs.Specification
import org.specs.mock.Mockito
import cluster._

abstract class BaseNetworkClientSpecification extends Specification with Mockito {
  val clusterClient = mock[ClusterClient]
  val networkClient: BaseNetworkClient

  val nodes = List(Node(1, "", true), Node(2, "", true), Node(3, "", true))
  val nodeSet = Set() ++ nodes
  val message = mock[Message]

  def sharedFunctionality = {
    "start the cluster, creates a load balancer and register itself as a listener when started" in {
      val cc = mock[ClusterClient]
      cc.addListener(any[ClusterListener]) returns ClusterListenerKey(1)
      cc.nodes returns nodeSet

      val nc = new BaseNetworkClient with ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent {
        val clusterIoClient = mock[ClusterIoClient]
        val messageRegistry = mock[MessageRegistry]
        val clusterClient = cc
        var updateLoadBalancerCalled = false

        protected def updateLoadBalancer(nodes: Set[Node]) = updateLoadBalancerCalled = true
      }


      nc.start

      got {
        one(cc).start
        one(cc).addListener(any[ClusterListener])
        one(cc).nodes
      }
      nc.updateLoadBalancerCalled must beTrue
    }

    "update the load balancer when a Connected or NodesChanged event is received" in {
      val cc = mock[ClusterClient]
      cc.addListener(any[ClusterListener]) returns ClusterListenerKey(1)
      cc.nodes returns nodeSet
      var listener: ClusterListener = null
      cc.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
      cc.nodes returns nodeSet

      val nc = new BaseNetworkClient with ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent {
        val clusterIoClient = mock[ClusterIoClient]
        val messageRegistry = mock[MessageRegistry]
        val clusterClient = cc
        var updateLoadBalancerCalled = 0

        protected def updateLoadBalancer(nodes: Set[Node]) = {
          updateLoadBalancerCalled += 1
        }
      }

      nc.start

      listener must notBeNull
      listener.handleClusterEvent(ClusterEvents.Connected(nodeSet))
      listener.handleClusterEvent(ClusterEvents.NodesChanged(nodeSet))
      nc.updateLoadBalancerCalled must be_==(3)
    }

    "update nodes in clusterIoClient when nodes change" in {
      val cc = mock[ClusterClient]
      cc.addListener(any[ClusterListener]) returns ClusterListenerKey(1)
      cc.nodes returns nodeSet
      var listener: ClusterListener = null
      cc.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
      cc.nodes returns nodeSet

      val nc = new BaseNetworkClient with ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent {
        val clusterIoClient = mock[ClusterIoClient]
        val messageRegistry = mock[MessageRegistry]
        val clusterClient = cc
        var updateLoadBalancerCalled = 0

        protected def updateLoadBalancer(nodes: Set[Node]) = {
          updateLoadBalancerCalled += 1
        }
      }

      doNothing.when(nc.clusterIoClient).nodesChanged(Set())

      nc.start
      listener.handleClusterEvent(ClusterEvents.NodesChanged(Set()))

      there was one(nc.clusterIoClient).nodesChanged(Set())
    }

    "shut down the clusterIoClient and unregister from the cluster when shutdown is called" in {
      val cc = mock[ClusterClient]
      val key = ClusterListenerKey(1)
      cc.addListener(any[ClusterListener]) returns key
      cc.nodes returns nodeSet

      val nc = new BaseNetworkClient with ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent {
        val clusterIoClient = mock[ClusterIoClient]
        val messageRegistry = mock[MessageRegistry]
        val clusterClient = cc

        protected def updateLoadBalancer(nodes: Set[Node]) = null
      }

      nc.start
      nc.shutdown

      got {
        one(nc.clusterIoClient).shutdown
        one(cc).removeListener(key)
      }
    }

    "do nothing if shutdown is called before start" in {
      val cc = mock[ClusterClient]
      val key = ClusterListenerKey(1)
      cc.addListener(any[ClusterListener]) returns key
      doNothing.when(cc).removeListener(any[ClusterListenerKey])

      val nc = new BaseNetworkClient with ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent {
        val clusterIoClient = mock[ClusterIoClient]
        val messageRegistry = mock[MessageRegistry]
        val clusterClient = cc

        protected def updateLoadBalancer(nodes: Set[Node]) = null
      }

      nc.shutdown

      there was no(cc).removeListener(any[ClusterListenerKey])
      there was no(nc.clusterIoClient).shutdown
    }

    "shut down the clusterIoClient when a Shutdown event is called" in {
      val cc = mock[ClusterClient]
      cc.addListener(any[ClusterListener]) returns ClusterListenerKey(1)
      cc.nodes returns nodeSet
      var listener: ClusterListener = null
      cc.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }

      val nc = new BaseNetworkClient with ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent {
        val clusterIoClient = mock[ClusterIoClient]
        val messageRegistry = mock[MessageRegistry]
        val clusterClient = cc

        protected def updateLoadBalancer(nodes: Set[Node]) = null
      }

      nc.start

      listener.handleClusterEvent(ClusterEvents.Shutdown)
      there was one(nc.clusterIoClient).shutdown
    }

    "send a message to every available node for broadcastMessage" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
//      nodes.foreach(n => doNothing.when(clusterIoClient).sendMessage(n, message, null))

      networkClient.start
      networkClient.broadcastMessage(message) must notBeNull

//      nodes.foreach(n => clusterIoClient.sendMessage(n, message, null) was called)
    }

    "send message to the specified node in sendMessageToNode" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessageToNode(message, nodes(1)) must notBeNull

//      clusterIoClient.sendMessage(node, message, null) was called
    }

    "throw an InvalidNodeException if the node provided to sendMessageToNode is not currently availabe" in {
      val node = Node(4, "", true)
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessageToNode(message, node) must throwA[InvalidNodeException]

//      clusterIoClient.sendMessage(node, message, null) wasnt called
    }
  }
}
