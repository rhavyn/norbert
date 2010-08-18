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
import java.util.concurrent.TimeUnit
import org.specs.util.WaitFor

class ClusterManagerClusterClientSpec extends Specification with WaitFor {
  val currentNodes = Set(Node(1, "localhost:31313"), Node(2, "localhost:31314"), Node(3, "localhost:31315"))

  var clusterManagerDelegate: ClusterManagerDelegate = null
  var clusterManager: ClusterManager = null

  var started = false
  var clusterManagerShouldFail = false
  var addedNode: Node = null
  var nodeIdFromMessage = 0
  var clusterManagerShutdownCount = 0

  val clusterClient = new ClusterManagerClusterClient {
    def serviceName = "test"

    protected def newClusterManager(delegate: ClusterManagerDelegate) = {
      clusterManagerDelegate = delegate

      clusterManager = new ClusterManager {
        protected val delegate = clusterManagerDelegate

        def act {
          import ClusterManagerMessages._

          started = true

          if (clusterManagerShouldFail) delegate.connectionFailed(new ClusterException)

          loop {
            react {
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
                exit
            }
          }
        }
      }

      clusterManager
    }
  }

  "An unconnected ClusterManagerClusterClient" should {
    doAfter {
      clusterManager ! ClusterManagerMessages.Shutdown
    }

    "when connect is called" in {
      "start the clusterManager" in {
        clusterClient.connect
        started must eventually(beTrue)
      }

      "throw an exception if there is an error" in {
        clusterManagerShouldFail = true
        clusterClient.connect
        clusterClient.awaitConnection must throwA[ClusterException]
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
      clusterManager ! ClusterManagerMessages.Shutdown
    }

    "when nodes is called" in {
      "return the nodes" in {
        clusterManagerDelegate.didConnect(currentNodes)
        clusterClient.nodes must be_==(currentNodes)
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.nodes must throwA[ClusterShutdownException]
      }
    }

    "when nodeWithId is called" in {
      "return the proper node" in {
        clusterManagerDelegate.didConnect(currentNodes)
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

    var connectedEventCount = 0
    val listener = ClusterListener {
      case ClusterEvents.Connected(_) => connectedEventCount += 1
    }

    "when addListener is called" in {
      "register the listener with the notification center" in {
        clusterManagerDelegate.didConnect(currentNodes)
        clusterClient.addListener(listener) must haveClass[ClusterListenerKey]
        connectedEventCount must eventually(be_==(1))
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.addListener(listener) must throwA[ClusterShutdownException]
      }
    }

    "when removeListener is called unregister the listener with the notification center" in {
      val key = clusterClient.addListener(listener)
      clusterManagerDelegate.didConnect(currentNodes)
      connectedEventCount must eventually(be_==(1))
      clusterClient.removeListener(key)
      clusterManagerDelegate.didConnect(currentNodes)
      waitFor(250.ms)
      connectedEventCount must eventually(be_==(1))
    }

    "when shutdown is called" in {
      "shut down the notification center and cluster manager" in {
        clusterClient.shutdown
        clusterManagerShutdownCount must eventually(be_==(1))
      }

      "throw a ClusterShutdownException if shutdown was already called" in {
        clusterClient.shutdown
        clusterClient.shutdown must throwA[ClusterShutdownException]
      }
    }
  }
}
