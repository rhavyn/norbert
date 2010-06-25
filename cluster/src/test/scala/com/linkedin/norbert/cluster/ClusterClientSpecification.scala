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

import org.specs.Specification
import java.util.concurrent.{TimeoutException, TimeUnit}
import org.specs.util.WaitFor

trait ClusterClientSpecification extends Specification with WaitFor {
  var clusterClient: ClusterClient = _

  var connectedEventCount = 0
  var connectedNodes: Set[Node] = null
  var nodesChanged: Set[Node] = null
  var nodesChangedEventCount = 0
  var shutdownEventCount = 0
  val listener = ClusterListener {
    case ClusterEvents.Connected(nodes) =>
      connectedEventCount += 1
      connectedNodes = nodes

    case ClusterEvents.NodesChanged(nodes) =>
      nodesChangedEventCount += 1
      nodesChanged = nodes

    case ClusterEvents.Shutdown => shutdownEventCount += 1
  }

  def newClusterClient: ClusterClient

  def initializeClusterClient {
    clusterClient = newClusterClient
  }

  def setup {
    nodesChanged = null
    nodesChangedEventCount = 0
    clusterClient.addListener(listener)
  }

  def cleanup {
    clusterClient.shutdown
  }

  def unconnectedClusterClientExamples = {
    "throw a NotYetConnectedException for" in {
      "nodes" in {
        clusterClient.nodes must throwA[NotYetConnectedException]
      }

      "nodeWithId" in {
        clusterClient.nodeWithId(1) must throwA[NotYetConnectedException]
      }

      "addNode" in {
        clusterClient.addNode(1, "localhost") must throwA[NotYetConnectedException]
      }

      "removeNode" in {
        clusterClient.removeNode(1) must throwA[NotYetConnectedException]
      }

      "markNodeAvailable" in {
        clusterClient.markNodeAvailable(1) must throwA[NotYetConnectedException]
      }

      "markNodeUnavailable" in {
        clusterClient.markNodeUnavailable(1) must throwA[NotYetConnectedException]
      }

      "isConnected" in {
        clusterClient.isConnected must throwA[NotYetConnectedException]
      }

      "awaitConnection" in {
        clusterClient.awaitConnection must throwA[NotYetConnectedException]
      }

      "awaitConnection with timeout" in {
        clusterClient.awaitConnection(1, TimeUnit.MILLISECONDS) must throwA[NotYetConnectedException]
      }

      "awaitConnectionUninterruptibly" in {
        clusterClient.awaitConnectionUninterruptibly must throwA[NotYetConnectedException]
      }

      "awaitConnectionUninterruptibly with timeout" in {
        clusterClient.awaitConnectionUninterruptibly(1, TimeUnit.MILLISECONDS) must throwA[NotYetConnectedException]
      }
    }

    "send a Connected event to listeners after the cluster is connected" in {
      var connectedEventCount = 0
      clusterClient.addListener(ClusterListener {
        case ClusterEvents.Connected(_) => connectedEventCount += 1
      })
      clusterClient.connect
      if (!clusterClient.awaitConnectionUninterruptibly(1, TimeUnit.SECONDS)) {
        throw new TimeoutException("Timed out wait for cluster connection")
      }
      connectedEventCount must eventually(be_==(1))
    }
  }

  def shutdownClusterClientExamples = {
    "throw a ClusterShutdownException for" in {
      "connect" in {
        clusterClient.connect must throwA[ClusterShutdownException]
      }

      "nodes" in {
        clusterClient.nodes must throwA[ClusterShutdownException]
      }

      "nodeWithId" in {
        clusterClient.nodeWithId(1) must throwA[ClusterShutdownException]
      }

      "addNode" in {
        clusterClient.addNode(1, "localhost") must throwA[ClusterShutdownException]
      }

      "removeNode" in {
        clusterClient.removeNode(1) must throwA[ClusterShutdownException]
      }

      "markNodeAvailable" in {
        clusterClient.markNodeAvailable(1) must throwA[ClusterShutdownException]
      }

      "markNodeUnavailable" in {
        clusterClient.markNodeUnavailable(1) must throwA[ClusterShutdownException]
      }

      "addListener" in {
        clusterClient.addListener(null) must throwA[ClusterShutdownException]
      }

      "addListener" in {
        clusterClient.removeListener(null) must throwA[ClusterShutdownException]
      }

      "isConnected" in {
        clusterClient.isConnected must throwA[ClusterShutdownException]
      }

      "awaitConnection" in {
        clusterClient.awaitConnection must throwA[ClusterShutdownException]
      }

      "awaitConnection with timeout" in {
        clusterClient.awaitConnection(1, TimeUnit.MILLISECONDS) must throwA[ClusterShutdownException]
      }

      "awaitConnectionUninterruptibly" in {
        clusterClient.awaitConnectionUninterruptibly must throwA[ClusterShutdownException]
      }

      "awaitConnectionUninterruptibly with timeout" in {
        clusterClient.awaitConnectionUninterruptibly(1, TimeUnit.MILLISECONDS) must throwA[ClusterShutdownException]
      }

      "shutdown" in {
        clusterClient.shutdown must throwA[ClusterShutdownException]
      }
    }

    "isShutdown must be true" in {
      clusterClient.isShutdown must beTrue
    }
  }

  def connectedClusterClientExamples = {
    "start empty" in {
      connectedEventCount must eventually(be_==(1))
      clusterClient.nodes must haveSize(0)
    }

    "when addNode is called" in {
      "add the node" in {
        val n1 = clusterClient.addNode(1, "localhost:31313")
        val n2 = clusterClient.addNode(2, "localhost:31314")
        n1 must notBeNull
        n2 must notBeNull
        val ns = clusterClient.nodes
        ns must haveSize(2)
        ns must containAll(Seq(n1, n2))
      }

      "send a NodesChanged event to listeners" in {
        clusterClient.addNode(1, "localhost:31313")
        nodesChangedEventCount must eventually(be_>=(1))
        nodesChanged must beEmpty
      }

      "throw an InvalidNodeException if the node already exists" in {
        clusterClient.addNode(1, "localhost:31313")
        clusterClient.addNode(1, "localhost:31313") must throwA[InvalidNodeException]
      }
    }

    "when removeNode is called" in {
      "remove the node" in {
        val n1 = clusterClient.addNode(1, "localhost:31313")
        val n2 = clusterClient.addNode(2, "localhost:31314")
        clusterClient.removeNode(1)
        val ns = clusterClient.nodes
        ns must haveSize(1)
        ns must notContain(n1)
        ns must contain(n2)
      }

      "send a NodesChanged event to listeners" in {
        clusterClient.addNode(1, "localhost:31313")
        nodesChangedEventCount must eventually(be_>=(1))
        nodesChangedEventCount = 0

        clusterClient.removeNode(1)
        nodesChangedEventCount must eventually(be_>=(1))
        nodesChanged must beEmpty
      }
    }

    "when markNodeAvailable is called" in {
      "mark the node available" in {
        clusterClient.addNode(1, "localhost:31313")
        clusterClient.nodes.foreach { _.available must beFalse }
        clusterClient.markNodeAvailable(1)
        clusterClient.nodes.foreach { _.available must beTrue }
      }

      "send a NodesChanged event to listeners" in {
        val n = clusterClient.addNode(1, "localhost:31313")
        clusterClient.markNodeAvailable(1)

        nodesChangedEventCount must eventually(be_>=(2))
        nodesChanged must eventually(haveSize(1))
        nodesChanged must contain(n)
      }
    }

    "when markNodeUnavailable is called" in {
      "mark the node unavailable" in {
        clusterClient.addNode(1, "localhost:31313")
        clusterClient.nodes.foreach { _.available must beFalse }
        clusterClient.markNodeAvailable(1)
        clusterClient.nodes.foreach { _.available must beTrue }
        clusterClient.markNodeUnavailable(1)
        clusterClient.nodes.foreach { _.available must beFalse }
      }

      "send a NodesChanged event to listeners" in {
        val n = clusterClient.addNode(1, "localhost:31313")
        clusterClient.markNodeAvailable(1)

        nodesChangedEventCount must eventually(be_>=(2))
        nodesChanged must eventually(haveSize(1))

        clusterClient.markNodeUnavailable(1)
        nodesChanged must eventually(haveSize(0))
      }
    }

    "when addListener is called add the listener and send it a Connected event" in {
      var connectedCount = 0
      clusterClient.addListener(ClusterListener {
        case ClusterEvents.Connected(_) => connectedCount += 1
      })
      connectedCount must eventually(be_==(1))
    }

    "when removeListener is called remove the listener" in {
      var connectedCount = 0
      var nodesChangedCount = 0
      val key = clusterClient.addListener(ClusterListener {
        case ClusterEvents.Connected(_) => connectedCount += 1
        case ClusterEvents.NodesChanged(_) => nodesChangedCount += 1
      })
      connectedCount must eventually(be_==(1))

      clusterClient.removeListener(key)
      clusterClient.addNode(1, "localhost:31313")
      waitFor(250.ms)
      nodesChangedCount must be_==(0)
    }

    "isConnected must be true" in {
      clusterClient.isConnected must beTrue
    }

    "when shutdown is called cluster listeners should recieve a shutdown event" in {
      clusterClient.shutdown
      shutdownEventCount must eventually(be_==(1))
    }
  }
}
