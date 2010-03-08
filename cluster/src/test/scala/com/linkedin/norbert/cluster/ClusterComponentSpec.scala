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
        with ZooKeeperManagerComponent with ClusterNotificationManagerComponent with RouterFactoryComponent
        with ClusterListenerComponent {

  type Id = Any
  val routerFactory = null

  val clusterListenerKey = ClusterListenerKey(10101L)
  val currentRouter = mock[Router]
  val currentNodes = Array(Node(1, new InetSocketAddress("localhost", 31313), Array(0, 1), true),
          Node(2, new InetSocketAddress("localhost", 31314), Array(0, 1), true),
          Node(3, new InetSocketAddress("localhost", 31315), Array(0, 1), true))
  var clusterActor: Actor = _
  var getCurrentNodesCount = 0
  var getCurrentRouterCount = 0
  var addListenerCount = 0
  var currentListeners: List[Actor] = Nil
  var removeListenerCount = 0
  var cnmShutdownCount = 0

  val clusterNotificationManager = actor {
    loop {
      react {
        case ClusterNotificationMessages.Connected(nodes) =>
        case ClusterNotificationMessages.AddListener(a) =>
          if (clusterActor == null) clusterActor = a
          else {
            addListenerCount += 1
            currentListeners = a :: currentListeners
          }
          reply(ClusterNotificationMessages.AddedListener(clusterListenerKey))

        case ClusterNotificationMessages.RemoveListener(key) => removeListenerCount += 1

        case ClusterNotificationMessages.GetCurrentNodes =>
          getCurrentNodesCount += 1
          reply(ClusterNotificationMessages.CurrentNodes(currentNodes))

        case ClusterNotificationMessages.GetCurrentRouter =>
          getCurrentRouterCount += 1
          reply(ClusterNotificationMessages.CurrentRouter(Some(currentRouter)))

        case ClusterNotificationMessages.Shutdown => cnmShutdownCount += 1
      }
    }
  }

  var addNodeCount = 0
  var nodeAdded: Node = _
  var removeNodeCount = 0
  var nodeRemovedId = 0
  var markNodeAvailableCount = 0
  var markNodeAvailableId = 0
  var markNodeUnavailableCount = 0
  var markNodeUnavailableId = 0
  var zkmShutdownCount = 0

  val zooKeeperManager = actor {
    loop {
      react {
        case ZooKeeperManagerMessages.AddNode(node) =>
          addNodeCount += 1
          nodeAdded = node
          reply(ZooKeeperManagerMessages.ZooKeeperManagerResponse(None))

        case ZooKeeperManagerMessages.RemoveNode(id) =>
          removeNodeCount += 1
          nodeRemovedId = id
          reply(ZooKeeperManagerMessages.ZooKeeperManagerResponse(None))

        case ZooKeeperManagerMessages.MarkNodeAvailable(id) =>
          markNodeAvailableCount += 1
          markNodeAvailableId = id
          reply(ZooKeeperManagerMessages.ZooKeeperManagerResponse(None))

        case ZooKeeperManagerMessages.MarkNodeUnavailable(id) =>
          markNodeUnavailableCount += 1
          markNodeUnavailableId = id
          reply(ZooKeeperManagerMessages.ZooKeeperManagerResponse(None))

        case ZooKeeperManagerMessages.Shutdown => zkmShutdownCount += 1
      }
    }
  }

  val cluster = new Cluster(clusterNotificationManager, zooKeeperManager)
  cluster.start

  "ClusterComponent" should {
    "when starting start the cluster notification and ZooKeeper manager actors" in {
      val cNM = new Actor {
        var started = false

        def act() = {
          started = true
          react {
            case ClusterNotificationMessages.AddListener(_) => reply(ClusterNotificationMessages.AddedListener(null))
          }
        }
      }

      val zKM = new Actor {
        var started = false

        def act() = started = true
      }

      val c = new Cluster(cNM, zKM)
      c.start

      cNM.started must beTrue
      zKM.started must beTrue
    }

    "start" in {
      "disconnected" in {
        cluster.isConnected must beFalse
      }

      "not shutdown" in {
        cluster.isShutdown must beFalse
      }
    }

    "throw ClusterShutdownException if shut down for nodes, nodeWith*, router, *Listener, await*" in {
      cluster.shutdown
      cluster.nodes must throwA[ClusterShutdownException]
      cluster.nodeWithAddress(new InetSocketAddress("localhost", 31313)) must throwA[ClusterShutdownException]
      cluster.nodeWithId(1) must throwA[ClusterShutdownException]
      cluster.router must throwA[ClusterShutdownException]
      cluster.addListener(null) must throwA[ClusterShutdownException]
      cluster.removeListener(null) must throwA[ClusterShutdownException]
      cluster.awaitConnection must throwA[ClusterShutdownException]
      cluster.awaitConnection(1, TimeUnit.SECONDS) must throwA[ClusterShutdownException]
      cluster.awaitConnectionUninterruptibly must throwA[ClusterShutdownException]
    }

    "throw ClusterDisconnectedException if disconnected for addNode, removeNode, markNodeAvailable" in {
      cluster.addNode(1, new InetSocketAddress("localhost", 31313), Array(0, 1)) must throwA[ClusterDisconnectedException]
      cluster.removeNode(1) must throwA[ClusterDisconnectedException]
      cluster.markNodeAvailable(1) must throwA[ClusterDisconnectedException]
    }

    "handle a connected event" in {
      clusterActor ! ClusterEvents.Connected(Nil, None)
      waitFor(10.ms)

      cluster.isConnected must beTrue
    }

    "handle a disconnected event" in {
      clusterActor ! ClusterEvents.Connected(Nil, None)
      waitFor(10.ms)
      cluster.isConnected must beTrue
      clusterActor ! ClusterEvents.Disconnected
      waitFor(10.ms)
      cluster.isConnected must beFalse
    }

    "addNode should add a node to ZooKeeperManager" in {
      clusterActor ! ClusterEvents.Connected(Nil, None)
      waitFor(10.ms)

      val isa = new InetSocketAddress("localhost", 31313)
      cluster.addNode(1, isa, Array(1, 2)) must notBeNull
      addNodeCount must be_==(1)
      nodeAdded.id must be_==(1)
      nodeAdded.address must be_==(isa)
      nodeAdded.available must be_==(false)
    }

    "removeNode should remove a node from ZooKeeperManager" in {
      clusterActor ! ClusterEvents.Connected(Nil, None)
      waitFor(10.ms)

      cluster.removeNode(1)
      removeNodeCount must be_==(1)
      nodeRemovedId must be_==(1)
    }

    "markNodeAvailable should mark a node available in ZooKeeperManager" in {
      clusterActor ! ClusterEvents.Connected(Nil, None)
      waitFor(10.ms)

      cluster.markNodeAvailable(11)
      markNodeAvailableCount must be_==(1)
      markNodeAvailableId must be_==(11)
    }

    "markNodeUnavailable should mark a node unavailable in ZooKeeperMonitor" in {
      clusterActor ! ClusterEvents.Connected(Nil, None)
      waitFor(10.ms)

      cluster.markNodeUnavailable(111)
      markNodeUnavailableCount must be_==(1)
      markNodeUnavailableId must be_==(111)
    }

    "nodes should ask the ClusterNotificationManager for the current node list" in {
      clusterActor ! ClusterEvents.Connected(Nil, None)
      waitFor(10.ms)

      val nodes = cluster.nodes
      nodes.length must be_==(3)
      nodes must containAll(currentNodes)
      getCurrentNodesCount must be_==(1)
    }

    "router should ask the ClusterNotificationManager for the current router" in {
      clusterActor ! ClusterEvents.Connected(Nil, None)
      waitFor(10.ms)

      cluster.router must beSome[Router].which(_ must be(currentRouter))
      getCurrentRouterCount must be_==(1)
    }

    "when handling nodeWithId" in {
      "return the node that matches the specified id" in {
        cluster.nodeWithId(2) must beSome[Node].which(_ must be_==(currentNodes(1)))
      }

      "return None if no matching id" in {
        cluster.nodeWithId(4) must beNone
      }
    }

    "when handling nodeWithAddress" in {
      "return the node that matches the specified address and port" in {
        cluster.nodeWithAddress(new InetSocketAddress("localhost", 31314)) must beSome[Node].which(_ must be_==(currentNodes(1)))
      }

      "return the node that matches the specified port" in {
        cluster.nodeWithAddress(new InetSocketAddress(31315)) must beSome[Node].which(_ must be_==(currentNodes(2)))
      }

      "return None if no matching address" in {
        cluster.nodeWithAddress(new InetSocketAddress(31316)) must beNone
      }
    }

    "send an AddListener message to ClusterNotificationManager for addListener" in {
      val listener = new ClusterListener {
        var callCount = 0
        def handleClusterEvent(event: ClusterEvent): Unit = callCount += 1
      }

      cluster.addListener(listener) must notBeNull
      addListenerCount must be_==(1)
      currentListeners.head ! ClusterEvents.Disconnected
      waitFor(10.ms)
      listener.callCount must be_==(1)
    }

    "send a RemoveListener message to ClusterNotificationManager for removeListener" in {
      cluster.removeListener(ClusterListenerKey(1L))
      waitFor(10.ms)
      removeListenerCount must be_==(1)
    }

    "shutdown ClusterNotificationManager and ZooKeeperManager when shut down" in {
      cluster.shutdown
      waitFor(10.ms)

      cnmShutdownCount must be_==(1)
      zkmShutdownCount must be_==(1)

      cluster.isShutdown must beTrue
    }
  }
}
