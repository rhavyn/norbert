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
package zookeeper

import org.specs.mock.Mockito
import java.io.IOException
import org.apache.zookeeper.data.Stat
import java.util.ArrayList
import cluster.common.{ClusterManagerMessages, ClusterManagerSpecification}
import org.apache.zookeeper._

class MockedZooKeeperClusterManagerSpec extends ClusterManagerSpecification with Mockito {
  var zooKeeperConnectionShouldSucceed = true
  val mockZooKeeper = mock[ZooKeeper]

  var zooKeeperConnectCount = 0
  def mockZooKeeperFactory(s: String, i: Int, w: Watcher) = {
    zooKeeperConnectCount += 1

    if (zooKeeperConnectionShouldSucceed) mockZooKeeper
    else throw new IOException
  }

  val clusterManager = new ZooKeeperClusterManager(delegate, "test", "", 0, mockZooKeeperFactory _)

  val rootNode = "/test"
  val membershipNode = rootNode + "/members"
  val availabilityNode = rootNode + "/available"

  import ZooKeeperMessages._
  import ClusterManagerMessages._

  "A disconnected ZooKeeperClusterManager with mock ZooKeeper" should {
    doAfter { cleanup }

    "connect to ZooKeeper when started" in {
      clusterManager.start
      zooKeeperConnectCount must eventually(be_==(1))
    }

    "call connectionFailed on the delegate if instantiating ZooKeeper failed" in {
      zooKeeperConnectionShouldSucceed = false
      clusterManager.start
      connectionFailedEventCount must be_==(1)
    }
  }

  "A connected ZooKeeperClusterManager with mock ZooKeeper" should {
    doBefore { setup }
    doAfter { cleanup }

    "handle ZooKeeperMessages" in {
      "when a Connected message is received" in {
        val znodes = List(rootNode, membershipNode, availabilityNode)

        "verify that Norbert's ZNodes are created by" in {
          "doing nothing if all the znodes exist" in {
            znodes.foreach {mockZooKeeper.exists(_, false) returns mock[Stat]}
            clusterManager ! Connected
            connectedEventCount must eventually(be_==(1))
            znodes.foreach {there was one(mockZooKeeper).exists(_, false)}
          }

          "creating the cluster, membership and availability nodes if they do not exist" in {
            znodes.foreach {path =>
              mockZooKeeper.exists(path, false) returns null
              mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns path
            }

            clusterManager ! Connected
            connectedEventCount must eventually(be_==(1))

            got {
              znodes.foreach {path =>
                one(mockZooKeeper).exists(path, false)
                one(mockZooKeeper).create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
              }
            }
          }
        }

        "calculate the current nodes and call didConnect on the delegate" in {
          val nodes = Seq(Node(1, "localhost:31313", true), Node(2, "localhost:31314", true), Node(3, "localhost:31315"))
          val membership = new ArrayList[String]
          nodes.foreach {n => membership.add(n.id.toString)}

          val availability = new ArrayList[String]
          nodes.dropRight(1).foreach {n => availability.add(n.id.toString)}

          mockZooKeeper.getChildren(membershipNode, true) returns membership
          nodes.foreach {n => mockZooKeeper.getData("%s/%d".format(membershipNode, n.id), false, null) returns n.toByteArray}
          mockZooKeeper.getChildren(availabilityNode, true) returns availability

          clusterManager ! Connected
          connectedEventCount must eventually(be_==(1))
          nodesFromEvent must haveSize(3)
          nodesFromEvent must containAll(nodes)

          got {
            one(mockZooKeeper).getChildren(membershipNode, true)
            nodes.foreach {n => one(mockZooKeeper).getData("%s/%d".format(membershipNode, n.id), false, null)}
            one(mockZooKeeper).getChildren(availabilityNode, true)
          }
        }

        "call connectionFailed on the delegate if verifying Norbert's nodes fails" in {
          znodes.foreach {mockZooKeeper.exists(_, false) throws new KeeperException.ConnectionLossException()}
          clusterManager ! Connected
          connectionFailedEventCount must eventually(be_==(1))
        }
      }

      "when a NodesChanged message is received" in {
        "and availability changed" in {
          "update the node availability and call nodesDidChange on the delegate" in {
            val nodes = Seq(Node(1, "localhost:31313"), Node(2, "localhost:31314"), Node(3, "localhost:31315"))
            val membership = new ArrayList[String]
            nodes.foreach {n => membership.add(n.id.toString)}

            val availability = new ArrayList[String]
            availability.add("2")

            val newAvailability = new ArrayList[String]
            Seq(1, 3).foreach {n => newAvailability.add(n.toString)}

            mockZooKeeper.getChildren(membershipNode, true) returns membership
            nodes.foreach {n => mockZooKeeper.getData("%s/%d".format(membershipNode, n.id), false, null) returns n.toByteArray}
            mockZooKeeper.getChildren(availabilityNode, true) returns availability thenReturns newAvailability

            clusterManager ! Connected
            connectedEventCount must eventually(be_==(1))
            nodesFromEvent must haveSize(3)
            nodesFromEvent.foreach {n => n.available must be_==(n.id == 2)}

            clusterManager ! NodeChildrenChanged(availabilityNode)
            nodesChangedEventCount must eventually(be_==(1))
            nodesFromEvent.foreach {n => n.available must be_==(n.id != 2)}

            there were two(mockZooKeeper).getChildren(availabilityNode, true)
          }

          "handle the case that all nodes are unavailable" in {
            val nodes = Seq(Node(1, "localhost:31313"), Node(2, "localhost:31314"), Node(3, "localhost:31315"))
            val membership = new ArrayList[String]
            nodes.foreach {n => membership.add(n.id.toString)}

            mockZooKeeper.getChildren(membershipNode, true) returns membership
            nodes.foreach {n => mockZooKeeper.getData("%s/%d".format(membershipNode, n.id), false, null) returns n.toByteArray}
            mockZooKeeper.getChildren(availabilityNode, true) returns membership thenReturns new ArrayList[String]

            clusterManager ! Connected
            connectedEventCount must eventually(be_==(1))
            nodesFromEvent.foreach {_.available must beTrue}

            clusterManager ! NodeChildrenChanged(availabilityNode)
            nodesChangedEventCount must eventually(be_==(1))
            nodesFromEvent.foreach {_.available must beFalse}
          }

          "if disconnected from ZooKeeper do nothing" in {
            clusterManager !? (250, NodeChildrenChanged(availabilityNode))
            nodesChangedEventCount must be_==(0)
          }
        }

        "and the membership changed" in {
          "update the nodes and call nodesDidChange on the delegate" in {
            val nodes = Seq(Node(1, "localhost:31313", true), Node(2, "localhost:31314", true))
            val updatedNodes = nodes :+ Node(3, "localhostt:31315")

            val membership = new ArrayList[String]
            nodes.foreach {n => membership.add(n.id.toString)}

            val newMembership = new ArrayList[String]
            updatedNodes.foreach {n => newMembership.add(n.id.toString)}

            mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
            updatedNodes.foreach {n => mockZooKeeper.getData("%s/%d".format(membershipNode, n.id), false, null) returns n.toByteArray}
            mockZooKeeper.getChildren(availabilityNode, true) returns membership

            clusterManager ! Connected
            connectedEventCount must eventually(be_==(1))
            nodesFromEvent must haveSize(nodes.size)
            nodesFromEvent must containAll(nodes)

            clusterManager ! NodeChildrenChanged(membershipNode)
            nodesChangedEventCount must eventually(be_==(1))
            nodesFromEvent must haveSize(updatedNodes.size)
            nodesFromEvent must containAll(updatedNodes)

            got {
              two(mockZooKeeper).getChildren(availabilityNode, true)
              two(mockZooKeeper).getChildren(membershipNode, true)
            }
          }

          "handle the case that a node is removed" in {
            val nodes = Seq(Node(1, "localhost:31313", true), Node(2, "localhost:31314", true), Node(3, "localhost:31315", true))

            val membership = new ArrayList[String]
            nodes.foreach {n => membership.add(n.id.toString)}

            val newMembership = new ArrayList[String]
            Seq(1, 3).foreach {n => newMembership.add(n.toString)}

            mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
            nodes.foreach {n => mockZooKeeper.getData("%s/%d".format(membershipNode, n.id), false, null) returns n.toByteArray}
            mockZooKeeper.getChildren(availabilityNode, true) returns membership

            clusterManager ! Connected
            connectedEventCount must eventually(be_==(1))
            nodesFromEvent must haveSize(nodes.size)
            nodesFromEvent must containAll(nodes)
            nodesFromEvent.foreach {_.available must beTrue}

            clusterManager ! NodeChildrenChanged(membershipNode)
            nodesChangedEventCount must eventually(be_==(1))
            nodesFromEvent must haveSize(newMembership.size)
            nodesFromEvent must containAll(Seq(nodes(0), nodes(2)))

            there were two(mockZooKeeper).getChildren(membershipNode, true)
          }

          "if disconnected from ZooKeeper do nothing" in {
            clusterManager !? (250, NodeChildrenChanged(membershipNode))
            nodesChangedEventCount must be_==(0)
          }
        }
      }

      "when a Disconnected message is received" in {
        "call didDisconnect on the delegate" in {
          clusterManager ! Connected
          clusterManager ! Disconnected
          disconnectedEventCount must eventually(be_==(1))
        }

        "if disconnected from ZooKeeper do nothing" in {
          clusterManager !? (250, Disconnected)
          disconnectedEventCount must be_==(0)
        }
      }

      "when an Expired message is received in " in {
        "reconnect to ZooKeeper" in {
          zooKeeperConnectCount must eventually(be_==(1))
          clusterManager ! Expired
          zooKeeperConnectCount must eventually(be_==(2))
        }

        "call didDisconnect on the delegate" in {
          clusterManager ! Expired
          disconnectedEventCount must eventually(be_==(1))
        }
      }
    }

    "handle ClusterManagerMessages" in {
      val node = Node(1, "localhost:31313")
      val membershipPath = "%s/%d".format(membershipNode, node.id)
      val availabilityPath = "%s/%d".format(availabilityNode, node.id)

      "when an AddNode message is received" in {
        "add the node to ZooKeeper and call nodesDidChange on the delegate" in {
          mockZooKeeper.exists(membershipPath, false) returns null
          mockZooKeeper.create(membershipPath, node.toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns membershipPath

          clusterManager ! Connected
          clusterManager !? (1000, AddNode(node)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
          nodesChangedEventCount must be_==(1)
          nodesFromEvent must haveSize(1)
          nodesFromEvent must contain(node)

          got {
            one(mockZooKeeper).exists(membershipPath, false)
            one(mockZooKeeper).create(membershipPath, node.toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          }
        }

        "return an InvalidNodeException if the node exists" in {
          mockZooKeeper.exists(membershipPath, false) returns mock[Stat]

          clusterManager ! Connected
          clusterManager !? (1000, AddNode(node)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _ must haveClass[InvalidNodeException] }
          }
        }

        "handle ZooKeeper throwing an exception" in {
          mockZooKeeper.exists(membershipPath, false) throws KeeperException.create(KeeperException.Code.CONNECTIONLOSS, membershipPath)

          clusterManager ! Connected
          clusterManager !? (1000, AddNode(node)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _.getCause must haveSuperClass[KeeperException] }
          }
        }

        "return a ClusterDisconnectedException if disconnected from ZooKeeper" in {
          clusterManager !? (1000, AddNode(node)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _ must haveClass[ClusterDisconnectedException] }
          }
        }
      }

      "when a RemoveNode message is received" in {
        "remove the node and call nodesDidChange on the delegate" in {
          mockZooKeeper.exists(membershipPath, false) returns mock[Stat]
          doNothing.when(mockZooKeeper).delete(membershipPath, -1)

          clusterManager ! Connected
          clusterManager !? (1000, RemoveNode(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
          nodesChangedEventCount must be_==(1)
          nodesFromEvent must haveSize(0)

          got {
            one(mockZooKeeper).exists(membershipPath, false)
            one(mockZooKeeper).delete(membershipPath, -1)
          }
        }

        "do nothing if the node doesn't exist" in {
          mockZooKeeper.exists(membershipPath, false) returns null
          doNothing.when(mockZooKeeper).delete(membershipPath, -1)

          clusterManager ! Connected
          clusterManager !? (1000, RemoveNode(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }

          got {
            one(mockZooKeeper).exists(membershipPath, false)
            no(mockZooKeeper).delete(membershipPath, -1)
          }
        }

        "handle ZooKeeper throwing an exception" in {
          mockZooKeeper.exists(membershipPath, false) throws KeeperException.create(KeeperException.Code.CONNECTIONLOSS, membershipPath)

          clusterManager ! Connected
          clusterManager !? (1000, RemoveNode(node.id)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _.getCause must haveSuperClass[KeeperException] }
          }
        }

        "return a ClusterDisconnectedException if disconnected from ZooKeeper" in {
          clusterManager !? (1000, RemoveNode(node.id)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _ must haveClass[ClusterDisconnectedException] }
          }
        }
      }

      "when a MarkNodeAvailable message is received" in {
        "mark the node available and call nodesDidChange on the delegate" in {
          mockZooKeeper.exists(availabilityPath, false) returns null
          mockZooKeeper.create(availabilityPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns availabilityPath

          clusterManager ! Connected
          clusterManager !? (1000, AddNode(node))
          nodesChangedEventCount = 0
          clusterManager !? (1000, MarkNodeAvailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
          nodesChangedEventCount must be_==(1)

          got {
            one(mockZooKeeper).exists(availabilityPath, false)
            one(mockZooKeeper).create(availabilityPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          }
        }

        "do nothing if the node doesn't exist" in {
          mockZooKeeper.exists(availabilityPath, false) returns null
          mockZooKeeper.create(availabilityPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns availabilityPath

          clusterManager ! Connected
          clusterManager !? (1000, MarkNodeAvailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
          nodesChangedEventCount must be_==(0)
        }

        "do nothing if the node is already available" in {
          mockZooKeeper.exists(availabilityPath, false) returns mock[Stat]
          mockZooKeeper.create(availabilityPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns availabilityPath

          clusterManager ! Connected
          clusterManager !? (1000, MarkNodeAvailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }

          got {
            one(mockZooKeeper).exists(availabilityPath, false)
            no(mockZooKeeper).create(availabilityPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          }
        }

        "handle ZooKeeper throwing an exception" in {
          mockZooKeeper.exists(availabilityPath, false) throws KeeperException.create(KeeperException.Code.CONNECTIONLOSS, membershipPath)

          clusterManager ! Connected
          clusterManager !? (1000, MarkNodeAvailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _.getCause must haveSuperClass[KeeperException] }
          }
        }

        "return a ClusterDisconnectedException if disconnected from ZooKeeper" in {
          clusterManager !? (1000, MarkNodeAvailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _ must haveClass[ClusterDisconnectedException] }
          }
        }
      }

      "when a MarkNodeUnavailable message is received" in {
        "mark the node unavailable and call nodesDidChange on the delegate" in {
          mockZooKeeper.exists(availabilityPath, false) returns mock[Stat]
          doNothing.when(mockZooKeeper).delete(availabilityPath, -1)

          clusterManager ! Connected
          clusterManager !? (1000, AddNode(node))
          clusterManager !? (1000, MarkNodeAvailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
          nodesChangedEventCount = 0
          clusterManager !? (1000, MarkNodeUnavailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
          nodesChangedEventCount must be_==(1)

          got {
            two(mockZooKeeper).exists(availabilityPath, false)
            one(mockZooKeeper).delete(availabilityPath, -1)
          }
        }

        "do nothing if the node doesn't exist" in {
          mockZooKeeper.exists(availabilityPath, false) returns mock[Stat]
          doNothing.when(mockZooKeeper).delete(availabilityPath, -1)

          clusterManager ! Connected
          clusterManager !? (1000, MarkNodeUnavailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
          nodesChangedEventCount must be_==(0)
        }

        "do nothing if the node is already unavailable" in {
          mockZooKeeper.exists(availabilityPath, false) returns null
          doNothing.when(mockZooKeeper).delete(availabilityPath, -1)

          clusterManager ! Connected
          clusterManager !? (1000, MarkNodeUnavailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }

          got {
            no(mockZooKeeper).exists(availabilityPath, false)
            no(mockZooKeeper).delete(availabilityPath, -1)
          }
        }

        "handle ZooKeeper throwing an exception" in {
          mockZooKeeper.exists(availabilityPath, false) returns null thenReturns mock[Stat]
          mockZooKeeper.delete(availabilityPath, -1) throws KeeperException.create(KeeperException.Code.CONNECTIONLOSS, membershipPath)

          clusterManager ! Connected
          clusterManager !? (1000, MarkNodeAvailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) => o must beNone }
          clusterManager !? (1000, MarkNodeUnavailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _.getCause must haveSuperClass[KeeperException] }
          }
        }

        "return a ClusterDisconnectedException if disconnected from ZooKeeper" in {
          clusterManager !? (1000, MarkNodeUnavailable(node.id)) must beSomething.which { case ClusterManagerResponse(o) =>
            o must beSome[ClusterException].which { _ must haveClass[ClusterDisconnectedException] }
          }
        }
      }

      "when a Shutdown message is recieved, close the connection to ZooKeeper" in {
        doNothing.when(mockZooKeeper).close

        clusterManager ! Shutdown

        there was one(mockZooKeeper).close
      }
    }
  }
}
