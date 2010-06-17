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
package cluster
package common

import org.specs.Specification
import actors.Actor._
import java.util.concurrent.TimeUnit

class ClusterManagerClusterClientSpec extends Specification {
  val currentNodes = Set(Node(1, "localhost:31313"), Node(2, "localhost:31314"), Node(3, "localhost:31315"))

  val clusterClient = new ClusterManagerClusterClient with ClusterManagerComponent {
    import ClusterManagerMessages._
    import NotificationCenterMessages._

    var addListenerCount = 0
    var addedClusterListener: ClusterListener = null
    var removedListenerCount = 0
    var removedClusterListenerKey: ClusterListenerKey = null
    var notificationCenterShutdownCount = 0
    var clusterClientListener: ClusterListener = null
    val notificationCenter = actor {
      loop {
        react {
          case AddListener(listener) =>
            if (clusterClientListener == null) {
              clusterClientListener = listener
            } else {
              addedClusterListener = listener
              addListenerCount += 1
            }
            reply(AddedListener(ClusterListenerKey(1L)))

          case RemoveListener(key) =>
            removedClusterListenerKey = key
            removedListenerCount += 1

          case NotificationCenterMessages.Shutdown =>
            notificationCenterShutdownCount += 1
            reply(NotificationCenterMessages.Shutdown)
            exit
        }
      }
    }

    var connectMessageCount = 0
    var clusterManagerShouldFail = false
    var addedNode: Node = null
    var nodeIdFromMessage = 0
    var clusterManagerShutdownCount = 0
    val clusterManager = actor {
      loop {
        react {
          case Connect =>
            connectMessageCount += 1
            reply(ClusterManagerResponse(if (clusterManagerShouldFail) Some(new ClusterException) else None))

          case GetNodes =>
            if (clusterManagerShouldFail) {
              reply(ClusterManagerResponse(Some(new ClusterException)))
            } else {
              reply(Nodes(currentNodes))
            }

          case AddNode(node) =>
            addedNode = node
            reply(ClusterManagerResponse(if (clusterManagerShouldFail) Some(new ClusterException) else None))

          case RemoveNode(nodeId) =>
            nodeIdFromMessage = nodeId
            reply(ClusterManagerResponse(if (clusterManagerShouldFail) Some(new ClusterException) else None))

          case MarkNodeAvailable(nodeId) =>
            nodeIdFromMessage = nodeId
            reply(ClusterManagerResponse(if (clusterManagerShouldFail) Some(new ClusterException) else None))

          case MarkNodeUnavailable(nodeId) =>
            nodeIdFromMessage = nodeId
            reply(ClusterManagerResponse(if (clusterManagerShouldFail) Some(new ClusterException) else None))

          case ClusterManagerMessages.Shutdown =>
            clusterManagerShutdownCount += 1
            reply(ClusterManagerMessages.Shutdown)
            exit
        }
      }
    }

    def serviceName = "test"
  }

  import clusterClient._

  "An unconnected ClusterManagerClusterClient" should {
    doAfter {
      notificationCenter ! NotificationCenterMessages.Shutdown
      clusterManager ! ClusterManagerMessages.Shutdown
    }

    "when connect is called" in {
      "register as a ClusterListener" in {
        clusterClient.connect
        clusterClientListener must eventually(notBeNull)
      }

      "send a Connect message to the cluster manager" in {
        clusterClient.connect
        connectMessageCount must eventually(be_==(1))
      }

      "throw an exception if there is an error" in {
        clusterManagerShouldFail = true
        clusterClient.connect must throwA[ClusterException]
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.connect must throwA[ClusterShutdownException]
      }
    }

    "throw NotYetConnectedException" in {
      "when nodes is called" in {
        clusterClient.nodes must throwA[NotYetConnectedException]
      }

      "when nodeWithId is called" in {
        clusterClient.nodeWithId(1) must throwA[NotYetConnectedException]
      }

      "when addNode is called" in {
        clusterClient.addNode(1, "localhost:31313") must throwA[NotYetConnectedException]
      }

      "when removeNode is called" in {
        clusterClient.removeNode(1) must throwA[NotYetConnectedException]
      }

      "when markNodeAvailable is called" in {
        clusterClient.markNodeAvailable(1) must throwA[NotYetConnectedException]
      }

      "when markNodeUnavailable is called" in {
        clusterClient.markNodeAvailable(1) must throwA[NotYetConnectedException]
      }

      "when isConnected is called" in {
        clusterClient.isConnected must throwA[NotYetConnectedException]
      }

      "when awaitConnection is called" in {
        clusterClient.awaitConnection must throwA[NotYetConnectedException]
      }

      "when awaitConnection with a timeout is called" in {
        clusterClient.awaitConnection(1, TimeUnit.MILLISECONDS) must throwA[NotYetConnectedException]
      }

      "when awaitConnectionUninterruptibly is called" in {
        clusterClient.awaitConnectionUninterruptibly must throwA[NotYetConnectedException]
      }

      "when awaitConnectionUninterruptibly with a timeout is called" in {
        clusterClient.awaitConnectionUninterruptibly(1, TimeUnit.MILLISECONDS) must throwA[NotYetConnectedException]
      }
    }
  }

  "A connected ClusterManagerClusterClient" should {
    doBefore {
      clusterClient.connect
    }

    doAfter {
      notificationCenter ! NotificationCenterMessages.Shutdown
      clusterManager ! ClusterManagerMessages.Shutdown
    }

    "when nodes is called" in {
      "return the nodes" in {
        clusterClient.nodes must be_==(currentNodes)
      }

      "handle a returned exception" in {
        clusterManagerShouldFail = true
        clusterClient.nodes must throwA[ClusterException]
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.nodes must throwA[ClusterShutdownException]
      }
    }

    "when nodeWithId is called" in {
      "return the proper node" in {
        clusterClient.nodeWithId(1) must beSome[Node].which { _.id must be_==(1) }
      }

      "return None if no such node" in {
        clusterClient.nodeWithId(10) must beNone
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.nodeWithId(1) must throwA[ClusterShutdownException]
      }
    }

    "when addNode is called" in {
      "return the new node" in {
        var n = clusterClient.addNode(1, "localhost:31313")
        n.id must be_==(1)
        n.url must be_==("localhost:31313")
        n.partitionIds must beEmpty
        addedNode must be_==(n)

        n = clusterClient.addNode(2, "localhost:31314", Set(1, 3, 5, 7))
        n.id must be_==(2)
        n.url must be_==("localhost:31314")
        n.partitionIds must be_==(Set(1, 3, 5, 7))
        addedNode must be_==(n)
      }

      "throw an exception if there is an error" in {
        clusterManagerShouldFail = true
        clusterClient.addNode(1, "localhost:31313") must throwA[ClusterException]
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.addNode(1, "localhost:31313") must throwA[ClusterShutdownException]
      }
    }

    "when removeNode is called" in {
      "remove the node" in {
        clusterClient.removeNode(1210)
        nodeIdFromMessage must eventually(be_==(1210))
      }

      "throw an exception if there is an error" in {
        clusterManagerShouldFail = true
        clusterClient.removeNode(1) must throwA[ClusterException]
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.removeNode(1) must throwA[ClusterShutdownException]
      }
    }

    "when markNodeAvailable is called" in {
      "mark the node available" in {
        clusterClient.markNodeAvailable(1210)
        nodeIdFromMessage must eventually(be_==(1210))
      }

      "throw an exception if there is an error" in {
        clusterManagerShouldFail = true
        clusterClient.markNodeAvailable(1) must throwA[ClusterException]
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.markNodeAvailable(1) must throwA[ClusterShutdownException]
      }
    }

    "when markNodeUnavailable is called" in {
      "mark the node unavailable" in {
        clusterClient.markNodeUnavailable(1210)
        nodeIdFromMessage must eventually(be_==(1210))
      }

      "throw an exception if there is an error" in {
        clusterManagerShouldFail = true
        clusterClient.markNodeUnavailable(1) must throwA[ClusterException]
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.markNodeUnavailable(1) must throwA[ClusterShutdownException]
      }
    }

    "when addListener is called" in {
      val listener = ClusterListener {
        case ClusterEvents.Shutdown =>
      }

      "register the listener with the notification center" in {
        clusterClient.addListener(listener) must haveClass[ClusterListenerKey]
        addListenerCount must eventually(be_==(1))
        addedClusterListener must be_==(listener)
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.addListener(listener) must throwA[ClusterShutdownException]
      }
    }

    "when removeListener is called unregister the listener with the notification center" in {
      val key = ClusterListenerKey(1)
      clusterClient.removeListener(key)
      removedListenerCount must eventually(be_==(1))
      removedClusterListenerKey must be_==(key)
    }

    "when shutdown is called" in {
      "shut down the notification center and cluster manager" in {
        clusterClient.shutdown
        notificationCenterShutdownCount must eventually(be_==(1))
        clusterManagerShutdownCount must eventually(be_==(1))
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.shutdown must throwA[ClusterShutdownException]
      }
    }
  }
}
