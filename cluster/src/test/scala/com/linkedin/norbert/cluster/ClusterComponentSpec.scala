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
package com.linkedin.norbert.cluster

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import org.specs.SpecificationWithJUnit
import actors.Actor
import Actor._
import org.specs.util.WaitFor
import org.specs.mock.Mockito
import org.mockito.Matchers._

class ClusterComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with ClusterComponent
        with ClusterManagerComponent with ClusterWatcherComponent with ZooKeeperMonitorComponent
        with RouterFactoryComponent {

  val cluster = mock[Cluster]
  val zooKeeperMonitor = mock[ZooKeeperMonitor]
  val clusterWatcher = mock[ClusterWatcher]
  val clusterManager = new Actor {
    var shutdownCount = 0
    var addListenerCount = 0
    var removeListenerCount = 0
    var currentListener: ClusterListener = null

    def act() = {
      loop {
        react {
          case ClusterMessages.AddListener(l) =>
            currentListener = l
            addListenerCount += 1
          case ClusterMessages.RemoveListener(l) =>
            currentListener = null
            removeListenerCount += 1
          case ClusterMessages.Shutdown => shutdownCount += 1
        }
      }
    }
  }
  clusterManager.start

  type Id = Any
  val routerFactory = null

  "ClusterComponent" should {
    "start" in {
      "disconnected" in {
        val cluster = new DefaultCluster
        cluster.isConnected must beFalse
      }

      "not shutdown" in {
        val cluster = new DefaultCluster
        cluster.isShutdown must beFalse
      }
      
      "with an empty node list" in {
        val cluster = new DefaultCluster
        cluster.nodes must haveSize(0)
      }

      "with no router" in {
        val cluster = new DefaultCluster
        cluster.router must beNone
      }
    }
    
    "throw ClusterShutdownException if shut down for nodes, router, *Listener, await*" in {
      val cluster = new DefaultCluster

      cluster.shutdown
      cluster.nodes must throwA[ClusterShutdownException]
      cluster.router must throwA[ClusterShutdownException]
      cluster.addListener(null) must throwA[ClusterShutdownException]
      cluster.removeListener(null) must throwA[ClusterShutdownException]
      cluster.awaitConnection must throwA[ClusterShutdownException]
      cluster.awaitConnection(1, TimeUnit.SECONDS) must throwA[ClusterShutdownException]
      cluster.awaitConnectionUninterruptibly must throwA[ClusterShutdownException]
    }

    "throw ClusterDisconnectedException if disconnected for addNode, removeNode, markNodeAvailable, nodeWith" in {
      val cluster = new DefaultCluster

      cluster.addNode(1, new InetSocketAddress("localhost", 31313), Array(0, 1)) must throwA[ClusterDisconnectedException]
      cluster.removeNode(1) must throwA[ClusterDisconnectedException]
      cluster.markNodeAvailable(1) must throwA[ClusterDisconnectedException]
      cluster.nodeWithAddress(new InetSocketAddress("localhost", 31313)) must throwA[ClusterDisconnectedException]
      cluster.nodeWithId(1) must throwA[ClusterDisconnectedException]
    }

    "start zooKeeperMonitor when start is called" in {
      doNothing.when(zooKeeperMonitor).start()

      val cluster = new DefaultCluster
      cluster.start

      zooKeeperMonitor.start was called
    }

    "handle cluster events" in {
      "Connected updates the current state and make the cluster connected" in {
        val cluster = new DefaultCluster
        val nodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), false))

        cluster.handleClusterEvent(ClusterEvents.Connected(nodes, Some(mock[Router])))

        cluster.isConnected must beTrue
        cluster.nodes must haveTheSameElementsAs(nodes)
        cluster.router must beSome[Router]
      }

      "NodesChanged updates the current state" in {
        val cluster = new DefaultCluster
        val nodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), false))

        cluster.handleClusterEvent(ClusterEvents.NodesChanged(nodes, Some(mock[Router])))

        cluster.isConnected must beFalse // The connected state of the cluster shouldn't change
        cluster.nodes must haveTheSameElementsAs(nodes)
        cluster.router must beSome[Router]
      }

      "Disconnected disconnects the cluster and resets the current state" in {
        val cluster = new DefaultCluster
        val nodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), false))

        cluster.handleClusterEvent(ClusterEvents.NodesChanged(nodes, Some(mock[Router])))
        cluster.handleClusterEvent(ClusterEvents.Disconnected)

        cluster.isConnected must beFalse // The connected state of the cluster shouldn't change
        cluster.nodes must haveSize(0)
        cluster.router must beNone
      }
    }

    "add node should add a node to ZooKeeperMonitor" in {
      val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), false)

      zooKeeperMonitor.addNode(node.id, node.address, node.partitions) returns node

      val cluster = new DefaultCluster
      cluster.handleClusterEvent(ClusterEvents.Connected(Array(node), Some(mock[Router])))
      cluster.addNode(node.id, node.address, node.partitions) must be_==(node)

      zooKeeperMonitor.addNode(node.id, node.address, node.partitions) was called
    }

    "remove node should remove a node from ZooKeeperMonitor" in {
      val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), false)

      doNothing.when(zooKeeperMonitor).removeNode(node.id)

      val cluster = new DefaultCluster
      cluster.handleClusterEvent(ClusterEvents.Connected(Array(node), Some(mock[Router])))
      cluster.removeNode(node.id)

      zooKeeperMonitor.removeNode(node.id) was called
    }

    "mark node available should mark a node available to ZooKeeperMonitor" in {
      val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), false)

      doNothing.when(zooKeeperMonitor).markNodeAvailable(node.id)

      val cluster = new DefaultCluster
      cluster.handleClusterEvent(ClusterEvents.Connected(Array(node), Some(mock[Router])))
      cluster.markNodeAvailable(node.id)

      zooKeeperMonitor.markNodeAvailable(node.id) was called
    }

    "when handling nodeWithId" in {
      "return the node that matches the specified id" in {
        val nodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), true),
          Node(2, new InetSocketAddress("localhost", 31314), Array(0, 1), true),
          Node(3, new InetSocketAddress("localhost", 31315), Array(0, 1), true))

        val cluster = new DefaultCluster
        cluster.handleClusterEvent(ClusterEvents.Connected(nodes, Some(mock[Router])))
        cluster.nodeWithId(2) must beSome[Node].which(_ must be_==(nodes(1)))
      }

      "returns None if no matching id" in {
        val nodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), true),
          Node(2, new InetSocketAddress("localhost", 31314), Array(0, 1), true),
          Node(3, new InetSocketAddress("localhost", 31315), Array(0, 1), true))

        val cluster = new DefaultCluster
        cluster.handleClusterEvent(ClusterEvents.Connected(nodes, Some(mock[Router])))
        cluster.nodeWithId(4) must beNone
      }
    }

    "when handling nodeWithAddress" in {
      "return the node that matches the specified address and port" in {
        val nodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), true),
          Node(2, new InetSocketAddress("localhost", 31314), Array(0, 1), true),
          Node(3, new InetSocketAddress("localhost", 31315), Array(0, 1), true))

        val cluster = new DefaultCluster
        cluster.handleClusterEvent(ClusterEvents.Connected(nodes, Some(mock[Router])))
        cluster.nodeWithAddress(new InetSocketAddress("localhost", 31314)) must beSome[Node].which(_ must be_==(nodes(1)))
      }

      "return the node that matches the specified port" in {
        val nodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), true),
          Node(2, new InetSocketAddress("localhost", 31314), Array(0, 1), true),
          Node(3, new InetSocketAddress("localhost", 31315), Array(0, 1), true))

        val cluster = new DefaultCluster
        cluster.handleClusterEvent(ClusterEvents.Connected(nodes, Some(mock[Router])))
        cluster.nodeWithAddress(new InetSocketAddress(31315)) must beSome[Node].which(_ must be_==(nodes(2)))
      }

      "return None if no matching address" in {
        val nodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), true),
          Node(2, new InetSocketAddress("localhost", 31314), Array(0, 1), true),
          Node(3, new InetSocketAddress("localhost", 31315), Array(0, 1), true))

        val cluster = new DefaultCluster
        cluster.handleClusterEvent(ClusterEvents.Connected(nodes, Some(mock[Router])))
        cluster.nodeWithAddress(new InetSocketAddress(31316)) must beNone
      }
    }

    "send an AddListener message to ClusterManager for addListener" in {
      val listener = new ClusterListener {
        def handleClusterEvent(event: ClusterEvent) = null
      }
      val count = clusterManager.addListenerCount

      val cluster = new DefaultCluster
      cluster.addListener(listener)

      waitFor(10.ms)
      clusterManager.addListenerCount must be_==(count + 1)
      clusterManager.currentListener must be_==(listener)
    }

    "send a RemoveListener message to ClusterManager for removeListener" in {
      val listener = new ClusterListener {
        def handleClusterEvent(event: ClusterEvent) = null
      }
      val count = clusterManager.removeListenerCount

      val cluster = new DefaultCluster
      cluster.removeListener(listener)

      waitFor(10.ms)
      clusterManager.removeListenerCount must be_==(count + 1)
    }

    "shutdown ClusterManager, ClusterWatcher and ZooKeeperMonitor when shut down" in {
      doNothing.when(clusterWatcher).shutdown()
      doNothing.when(zooKeeperMonitor).shutdown()
      val count = clusterManager.shutdownCount

      val cluster = new DefaultCluster
      cluster.shutdown

      waitFor(10.ms)
      clusterManager.shutdownCount must be_==(count + 1)
      clusterWatcher.shutdown was called
      zooKeeperMonitor.shutdown was called
      cluster.isShutdown must beTrue
    }
  }
}
