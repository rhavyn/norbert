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

import java.util.concurrent.TimeUnit
import org.specs.SpecificationWithJUnit
import actors.Actor
import Actor._
import org.specs.util.WaitFor
import org.specs.mock.Mockito

class ClusterComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with ClusterComponent
        with ClusterNotificationManagerComponent with ClusterListenerComponent
        with ClusterManagerComponent {

  val clusterListenerKey = ClusterListenerKey(10101L)
  val currentNodes = Array(Node(1, "localhost:31313", Array(0, 1), true),
          Node(2, "localhost:31314", Array(0, 1), true),
          Node(3, "localhost:31315", Array(0, 1), true))
  var clusterActor: Actor = _
  var getCurrentNodesCount = 0
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
        case ClusterManagerMessages.AddNode(node) =>
          addNodeCount += 1
          nodeAdded = node
          reply(ClusterManagerMessages.ClusterManagerResponse(None))

        case ClusterManagerMessages.RemoveNode(id) =>
          removeNodeCount += 1
          nodeRemovedId = id
          reply(ClusterManagerMessages.ClusterManagerResponse(None))

        case ClusterManagerMessages.MarkNodeAvailable(id) =>
          markNodeAvailableCount += 1
          markNodeAvailableId = id
          reply(ClusterManagerMessages.ClusterManagerResponse(None))

        case ClusterManagerMessages.MarkNodeUnavailable(id) =>
          markNodeUnavailableCount += 1
          markNodeUnavailableId = id
          reply(ClusterManagerMessages.ClusterManagerResponse(None))

        case ClusterManagerMessages.Shutdown => zkmShutdownCount += 1
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

    "throw ClusterShutdownException if shut down for nodes, nodeWith*, *Listener, await*" in {
      cluster.shutdown
      cluster.nodes must throwA[ClusterShutdownException]
      cluster.nodeWithId(1) must throwA[ClusterShutdownException]
      cluster.addListener(null) must throwA[ClusterShutdownException]
      cluster.removeListener(null) must throwA[ClusterShutdownException]
      cluster.awaitConnection must throwA[ClusterShutdownException]
      cluster.awaitConnection(1, TimeUnit.SECONDS) must throwA[ClusterShutdownException]
      cluster.awaitConnectionUninterruptibly must throwA[ClusterShutdownException]
    }

    "throw ClusterDisconnectedException if disconnected for addNode, removeNode, markNodeAvailable" in {
      cluster.addNode(1, "localhost:31313", Array(0, 1)) must throwA[ClusterDisconnectedException]
      cluster.removeNode(1) must throwA[ClusterDisconnectedException]
      cluster.markNodeAvailable(1) must throwA[ClusterDisconnectedException]
    }

    "handle a connected event" in {
      clusterActor ! ClusterEvents.Connected(Nil)
      waitFor(10.ms)

      cluster.isConnected must beTrue
    }

    "handle a disconnected event" in {
      clusterActor ! ClusterEvents.Connected(Nil)
      waitFor(10.ms)
      cluster.isConnected must beTrue
      clusterActor ! ClusterEvents.Disconnected
      waitFor(10.ms)
      cluster.isConnected must beFalse
    }

    "addNode should add a node to ZooKeeperManager" in {
      clusterActor ! ClusterEvents.Connected(Nil)
      waitFor(10.ms)

      cluster.addNode(1, "localhost:31313", Array(1, 2)) must notBeNull
      addNodeCount must be_==(1)
      nodeAdded.id must be_==(1)
      nodeAdded.url must be_==("localhost:31313")
      nodeAdded.available must be_==(false)
    }

    "removeNode should remove a node from ZooKeeperManager" in {
      clusterActor ! ClusterEvents.Connected(Nil)
      waitFor(10.ms)

      cluster.removeNode(1)
      removeNodeCount must be_==(1)
      nodeRemovedId must be_==(1)
    }

    "markNodeAvailable should mark a node available in ZooKeeperManager" in {
      clusterActor ! ClusterEvents.Connected(Nil)
      waitFor(10.ms)

      cluster.markNodeAvailable(11)
      markNodeAvailableCount must be_==(1)
      markNodeAvailableId must be_==(11)
    }

    "markNodeUnavailable should mark a node unavailable in ZooKeeperMonitor" in {
      clusterActor ! ClusterEvents.Connected(Nil)
      waitFor(10.ms)

      cluster.markNodeUnavailable(111)
      markNodeUnavailableCount must be_==(1)
      markNodeUnavailableId must be_==(111)
    }

    "nodes should ask the ClusterNotificationManager for the current node list" in {
      clusterActor ! ClusterEvents.Connected(Nil)
      waitFor(10.ms)

      val nodes = cluster.nodes
      nodes.length must be_==(3)
      nodes must containAll(currentNodes)
      getCurrentNodesCount must be_==(1)
    }

    "when handling nodeWithId" in {
      "return the node that matches the specified id" in {
        cluster.nodeWithId(2) must beSome[Node].which(_ must be_==(currentNodes(1)))
      }

      "return None if no matching id" in {
        cluster.nodeWithId(4) must beNone
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
