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
import actors.Actor
import collection.immutable.Set
import org.specs.util.WaitFor

trait ClusterManagerSpecification extends Specification with WaitFor {
  val clusterManager: ClusterManager with Actor

  var connectedEventCount = 0
  var nodesChangedEventCount = 0
  var disconnectedEventCount = 0
  var shutdownEventCount = 0
  var connectionFailedEventCount = 0
  var nodesFromEvent = Set.empty[Node]

  val delegate = new ClusterManagerDelegate {
    def didShutdown = shutdownEventCount += 1

    def didDisconnect = disconnectedEventCount += 1

    def nodesDidChange(nodes: Set[Node]) = {
      nodesChangedEventCount += 1
      nodesFromEvent = nodes
    }

    def didConnect(nodes: Set[Node]) = {
      connectedEventCount += 1
      nodesFromEvent = nodes
    }

    def connectionFailed(ex: ClusterException) = connectionFailedEventCount += 1
  }

  def setup {
    clusterManager.start
  }

  def cleanup {
    clusterManager.shutdown
  }

  def clusterManagerExamples = {
    import ClusterManagerMessages._

    "call didConnected on delegate after connection is established" in {
      connectedEventCount must eventually(be_==(1))
    }

    "when an AddNode message is received" in {
      "add the node with availability set to false and call nodesDidChange on the delegate" in {
        clusterManager !? (1000, AddNode(Node(1, "localhost", true))) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        nodesChangedEventCount must be_==(1)
        nodesFromEvent must haveSize(1)
        nodesFromEvent must contain(Node(1, "localhost"))
      }

      "add the node with availability set to true if the node is already available" in {
        clusterManager !? (1000, MarkNodeAvailable(1))
        clusterManager !? (1000, AddNode(Node(1, "localhost", false))) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        nodesFromEvent must contain(Node(1, "localhost", true))
      }

      "respond with an InvalidNodeException if the node already exists" in {
        val node = Node(1, "localhost")
        clusterManager !? (1000, AddNode(node))
        clusterManager !? (1000, AddNode(node)) must beSomething.which { case ClusterManagerResponse(o) =>
          o must beSome[ClusterException].which { _ must haveClass[InvalidNodeException] }
        }
      }
    }

    "when a RemoveNode message is recieved" in {
      "remove the node and call nodesDidChange on the delegate" in {
        val node = Node(2, "localhost:2")
        val nodes = Seq(Node(1, "localhost:1"), node)
        nodes.foreach { n => clusterManager !? (1000, AddNode(n)) }
        nodesChangedEventCount = 0

        clusterManager !? (1000, RemoveNode(1)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        nodesChangedEventCount must be_==(1)
        nodesFromEvent must haveSize(1)
        nodesFromEvent must contain(node)
      }

      "not call nodesDidChange on the delegate if the node doesn't exist" in {
        val nodes = Seq(Node(1, "localhost:1"), Node(2, "localhost:2"))
        nodes.foreach { n => clusterManager !? (1000, AddNode(n)) }
        nodesChangedEventCount = 0

        clusterManager !? (1000, RemoveNode(3)) must beSomething
        nodesChangedEventCount must be_==(0)
        nodesFromEvent must haveSize(2)
        nodesFromEvent must containAll(nodes)
      }
    }

    "when a MarkNodeAvailable message is received" in {
      "mark the node available and call nodesDidChange on the delegate" in {
        val node = Node(1, "localhost")
        clusterManager !? (1000, AddNode(node))
        nodesChangedEventCount = 0

        clusterManager !? (1000, MarkNodeAvailable(1)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        nodesChangedEventCount must be_==(1)
        nodesFromEvent.foreach { _.available must beTrue }
      }

      "not call nodesDidChange on the delegate if the node doesn't exist" in {
        clusterManager !? (1000, MarkNodeAvailable(1))
        nodesChangedEventCount must be_==(0)
      }
    }

    "when a MarkNodeUnavailable message is received" in {
      "mark the node unavailable and call nodesDidChange on the delegate" in {
        val node = Node(1, "localhost")
        clusterManager !? (1000, AddNode(node))
        clusterManager !? (1000, MarkNodeAvailable(1))
        nodesChangedEventCount = 0

        clusterManager !? (1000, MarkNodeUnavailable(1)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        nodesChangedEventCount must be_==(1)
        nodesFromEvent.foreach { _.available must beFalse }
      }

      "not call nodesDidChange on the delegate if the node doesn't exist" in {
        clusterManager !? (1000, MarkNodeUnavailable(1))
        nodesChangedEventCount must be_==(0)
      }
    }

    "when a Shutdown message is received" in {
      "stop responding to messages" in {
        clusterManager ! Shutdown
        clusterManager !? (1000, AddNode(Node(1, "localhost")))
        nodesChangedEventCount must be_==(0)
      }

      "call didShutdown on the delegate" in {
        clusterManager ! Shutdown
        shutdownEventCount must eventually(be_==(1))
      }
    }
  }
}
