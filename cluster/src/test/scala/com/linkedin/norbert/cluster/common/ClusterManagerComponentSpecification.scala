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
import org.specs.util.WaitFor

trait ClusterManagerComponentSpecification extends Specification with WaitFor {
  var connectedEventCount = 0
  var nodesChangedEventCount = 0
  var disconnectedEventCount = 0
  var nodesFromEvent = Set.empty[Node]

  val notificationCenter = actor {
    loop {
      import NotificationCenterMessages._

      react {
        case SendConnectedEvent(n) =>
          connectedEventCount += 1
          nodesFromEvent = n

        case SendNodesChangedEvent(n) =>
          nodesChangedEventCount += 1
          nodesFromEvent = n

        case SendDisconnectedEvent => disconnectedEventCount += 1

        case 'quit => exit
      }
    }
  }

  val component: ClusterManagerComponent
  import component._
  import component.ClusterManagerMessages._

  def cleanup {
    notificationCenter ! 'quit
    clusterManager ! Shutdown
  }

  def connectedClusterManagerExamples = {
    "when a GetNode message is received return the current nodes" in {
      val nodes = Set(Node(1, "localhost:1"), Node(2, "localhost:2"))
      nodes.foreach { n => clusterManager !? (1000, AddNode(n)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone } }
      clusterManager !? (1000, GetNodes) must beSomething.which { case Nodes(n) =>
        n must haveSize(2)
        n must containAll(nodes)
      }
    }

    "when an AddNode message is received" in {
      "add the node with availability set to false" in {
        clusterManager !? (1000, AddNode(Node(1, "localhost", true))) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        clusterManager !? GetNodes match { case Nodes(n) =>
          n must haveSize(1)
          n.head.available must beFalse
        }
      }

      "add the node with availability set to true of the node is already available" in {
        clusterManager !? MarkNodeAvailable(1)
        clusterManager !? (1000, AddNode(Node(1, "localhost", false))) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        clusterManager !? GetNodes match { case Nodes(n) =>
          n must haveSize(1)
          n.head.available must beTrue
        }
      }

      "send a SendNodesChanged message to the notification center" in {
        val node = Node(1, "localhost")
        clusterManager ! AddNode(node)
        nodesChangedEventCount must eventually(be_>=(1))
        waitFor(250.ms)
        nodesFromEvent must haveSize(1)
        nodesFromEvent must contain(node)
      }

      "respond with an InvalidNodeException if the node already exists" in {
        val node = Node(1, "localhost")
        clusterManager !? AddNode(node)
        clusterManager !? (1000, AddNode(node)) must beSomething.which { case ClusterManagerResponse(o) =>
          o must beSome[ClusterException].which { _ must haveClass[InvalidNodeException] }
        }
      }
    }

    "when a RemoveNode message is recieved" in {
      "remove the node" in {
        val nodes = IndexedSeq(Node(1, "localhost:1"), Node(2, "localhost:2"))
        nodes.foreach { clusterManager !? AddNode(_) }
        clusterManager !? (1000, RemoveNode(1)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        clusterManager !? (1000, GetNodes) must beSomething.which { case Nodes(n) =>
          n must haveSize(1)
          n must contain(nodes(1))
        }
      }

      "send a SendNodesChanged message to the notification center" in {
        val nodes = IndexedSeq(Node(1, "localhost:1"), Node(2, "localhost:2"))
        nodes.foreach { clusterManager !? AddNode(_) }
        clusterManager !? RemoveNode(1)
        nodesChangedEventCount must eventually(be_>=(3))
        nodesFromEvent must haveSize(1)
        nodesFromEvent must contain(nodes(1))
      }
    }

    "when a MarkNodeAvailable message is received" in {
      "mark the node available" in {
        val node = Node(1, "localhost")
        clusterManager !? AddNode(node)
        clusterManager !? (1000, MarkNodeAvailable(1)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        clusterManager !? GetNodes match { case Nodes(n) => n.head.available must beTrue }
      }

      "send a SendNodesChanged message to the notification center" in {
        clusterManager !? MarkNodeAvailable(1)
        nodesChangedEventCount must eventually(be_>=(1))
        waitFor(250.ms)
        nodesFromEvent must haveSize(0)
      }
    }

    "when a MarkNodeUnavailable message is received" in {
      "mark the node unavailable" in {
        val node = Node(1, "localhost")
        clusterManager !? AddNode(node)
        clusterManager !? MarkNodeAvailable(1)
        clusterManager !? (1000, MarkNodeUnavailable(1)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
        clusterManager !? GetNodes match { case Nodes(n) => n.head.available must beFalse }
      }

      "send a SendNodesChanged message to the notification center" in {
        clusterManager !? MarkNodeAvailable(1)
        nodesChangedEventCount must eventually(be_>=(1))
        waitFor(250.ms)
        nodesFromEvent must haveSize(0)
      }
    }

    "when a Shutdown message is received stop responding to messages" in {
      clusterManager !? (1000, Shutdown) must beSomething
      clusterManager !? (1000, Shutdown) must beNone
    }
  }

  def unconnectedClusterManagerExamples = {
    "when a Connect message is received send a SendConnectedEvent to the notification center" in {
      clusterManager !? (1000, Connect) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
      connectedEventCount must eventually(be_==(1))
      nodesFromEvent must haveSize(0)
    }

    "return a NotYetConnectedException when" in {
      "a GetNodes message is received" in {
        clusterManager !? (1000, GetNodes) must beSomething.which { case ClusterManagerResponse(o) =>
          o must beSome[ClusterException].which { _ must haveClass[NotYetConnectedException] }
        }
      }

      "an AddNode message is received" in {
        clusterManager !? (1000, AddNode(Node(1, "localhost", true))) must beSomething.which { case ClusterManagerResponse(o) =>
          o must beSome[ClusterException].which { _ must haveClass[NotYetConnectedException] }
        }
      }

      "a RemoveNode message is received" in {
        clusterManager !? (1000, RemoveNode(1)) must beSomething.which { case ClusterManagerResponse(o) =>
          o must beSome[ClusterException].which { _ must haveClass[NotYetConnectedException] }
        }
      }

      "a MarkNodeAvailable message is received" in {
        clusterManager !? (1000, MarkNodeAvailable(1)) must beSomething.which { case ClusterManagerResponse(o) =>
          o must beSome[ClusterException].which { _ must haveClass[NotYetConnectedException] }
        }
      }

      "a MarkNodeUnavailable message is received" in {
        clusterManager !? (1000, MarkNodeUnavailable(1)) must beSomething.which { case ClusterManagerResponse(o) =>
          o must beSome[ClusterException].which { _ must haveClass[NotYetConnectedException] }
        }
      }
    }
  }
}
