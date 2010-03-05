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

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import actors.Actor
import Actor._
import org.specs.util.WaitFor
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper._
import java.util.ArrayList

class ZooKeeperManagerComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with ZooKeeperManagerComponent
        with ClusterNotificationManagerComponent with RouterFactoryComponent {
  import ZooKeeperManagerMessages._

  type Id = Int
  val routerFactory = null

  "ZooKeeperManager" should {
    val mockZooKeeper = mock[ZooKeeper]

    var connectedCount = 0
    var disconnectedCount = 0
    var nodesChangedCount = 0
    var shutdownCount = 0
    var nodesReceived: Seq[Node] = Nil

    val notificationActor = actor {
      loop {
        react {
          case ClusterNotificationMessages.Connected(nodes) => connectedCount += 1; nodesReceived = nodes
          case ClusterNotificationMessages.Disconnected => disconnectedCount += 1
          case ClusterNotificationMessages.NodesChanged(nodes) => nodesChangedCount += 1; nodesReceived = nodes
          case ClusterNotificationMessages.Shutdown => shutdownCount += 1
        }
      }
    }

    def zkf(connectString: String, sessionTimeout: Int, watcher: Watcher) = mockZooKeeper
    val zooKeeperManager = new ZooKeeperManager("", 0, "test", notificationActor)(zkf _)
    zooKeeperManager.start

    val rootNode = "/test"
    val membershipNode = rootNode + "/members"
    val availabilityNode = rootNode + "/available"

    "instantiate a ZooKeeper instance when started" in {
      var callCount = 0
      def countedZkf(connectString: String, sessionTimeout: Int, watcher: Watcher) = {
        callCount += 1
        mockZooKeeper
      }

      val zkm = new ZooKeeperManager("", 0, "", notificationActor)(countedZkf _)
      zkm.start
      waitFor(10.ms)

      callCount must be_==(1)
    }

    "when a Connected message is received" in {
      "verify the ZooKeeper structure by" in {
        val znodes = List(rootNode, membershipNode, availabilityNode)

        "doing nothing if all znodes exist" in {
          znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])

          zooKeeperManager ! Connected

          znodes.foreach(mockZooKeeper.exists(_, false) was called)
        }

        "creating the cluster, membership and availability znodes if they do not already exist" in {
          znodes.foreach { path =>
            mockZooKeeper.exists(path, false) returns null
            mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns path
          }

          zooKeeperManager ! Connected
          waitFor(10.ms)

          znodes.foreach { path =>
            mockZooKeeper.exists(path, false) was called
            mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) was called
          }
        }
      }

      "calculate the current nodes" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        availability.remove(2)

        val nodes = Array(Node(1, "localhost", 31313, Array(1, 2), true),
          Node(2, "localhost", 31314, Array(2, 3), false), Node(3, "localhost", 31315, Array(2, 3), true))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        zooKeeperManager ! Connected
        waitFor(10.ms)

        mockZooKeeper.getChildren(membershipNode, true) was called
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) was called
        }
        mockZooKeeper.getChildren(availabilityNode, true) was called          
      }

      "send a notification to the notification manager actor" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        availability.remove(1)

        val nodes = Array(Node(1, "localhost", 31313, Array(1, 2), true),
          Node(2, "localhost", 31314, Array(2, 3), false), Node(3, "localhost", 31315, Array(2, 3), true))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        zooKeeperManager ! Connected
        waitFor(10.ms)

        connectedCount must be_==(1)
        nodesReceived.length must be_==(3)
        nodesReceived must containAll(nodes)
        nodes.zip(nodesReceived.toArray).foreach { case (n1, n2) => n1.available must be_==(n2.available) }
      }
    }

    "when a Disconnected message is received" in {
      "send a notification to the notification manager actor" in {
        zooKeeperManager ! Connected
        zooKeeperManager ! Disconnected
        waitFor(10.ms)

        disconnectedCount must be_==(1)
      }

      "do nothing if not connected" in {
        zooKeeperManager ! Disconnected
        waitFor(10.ms)

        disconnectedCount must be_==(0)
      }
    }

    "when an Expired message is received" in {
      "reconnect to ZooKeeper" in {
        var callCount = 0
        def countedZkf(connectString: String, sessionTimeout: Int, watcher: Watcher) = {
          callCount += 1
          mockZooKeeper
        }

        val zkm = new ZooKeeperManager("", 0, "", notificationActor)(countedZkf _)
        zkm.start
        zkm ! Connected
        zkm ! Expired
        waitFor(10.ms)

        callCount must be_==(2)        
      }

      "send a notification to the notification manager actor" in {
        zooKeeperManager ! Connected
        zooKeeperManager ! Expired
        waitFor(10.ms)

        disconnectedCount must be_==(1)
      }

      "do nothing if not connected" in {
        zooKeeperManager ! Expired
        waitFor(10.ms)

        disconnectedCount must be_==(0)
      }
    }

    "when a NodeChildrenChanged message is received" in {
      "and the availability node changed" in {
        "update the node availability and notify listeners" in {
          val membership = new ArrayList[String]
          membership.add("1")
          membership.add("2")
          membership.add("3")

          val availability = membership.clone.asInstanceOf[ArrayList[String]]
          availability.remove(2)

          val nodes = Array(Node(1, "localhost", 31313, Array(1, 2), true),
            Node(2, "localhost", 31314, Array(2, 3), true), Node(3, "localhost", 31315, Array(2, 3), false))
          val updatedNodes = Array(nodes(0), nodes(1), Node(3, "localhost", 31315, Array(2, 3), true))

          mockZooKeeper.getChildren(membershipNode, true) returns membership
          nodes.foreach { node =>
            mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
          }
          mockZooKeeper.getChildren(availabilityNode, true) returns availability thenReturns membership

          zooKeeperManager ! Connected
          waitFor(10.ms)

          nodesReceived.length must be_==(3)
          nodesReceived must containAll(nodes)
          nodes.zip(nodesReceived.toArray).foreach { case (n1, n2) => n1.available must be_==(n2.available) }

          zooKeeperManager ! NodeChildrenChanged(availabilityNode)
          waitFor(10.ms)

          nodesChangedCount must be_==(1)
          nodesReceived.length must be_==(3)
          nodesReceived must containAll(updatedNodes)
          updatedNodes.zip(nodesReceived.toArray).foreach { case (n1, n2) => n1.available must be_==(n2.available) }

          mockZooKeeper.getChildren(availabilityNode, true) was called.twice
        }

        "do nothing if not connected" in {
          zooKeeperManager ! NodeChildrenChanged(availabilityNode)
          waitFor(10.ms)

          nodesChangedCount must be_==(0)
        }
      }

      "and the membership node changed" in {
        "update the nodes and notify listeners" in {
          val membership = new ArrayList[String]
          membership.add("1")
          membership.add("2")

          val newMembership = new ArrayList[String]
          newMembership.add("1")
          newMembership.add("2")
          newMembership.add("3")

          val updatedNodes = Array(Node(1, "localhost", 31313, Array(1, 2), true),
            Node(2, "localhost", 31314, Array(2, 3), true), Node(3, "localhost", 31315, Array(2, 3), false))
          val nodes = updatedNodes.slice(0, 2)

          mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
          updatedNodes.foreach { node =>
            mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
          }
          mockZooKeeper.getChildren(availabilityNode, true) returns membership

          zooKeeperManager ! Connected
          waitFor(10.ms)

          nodesReceived.length must be_==(2)
          nodesReceived must containAll(nodes)

          zooKeeperManager ! NodeChildrenChanged(membershipNode)
          waitFor(10.ms)

          nodesChangedCount must be_==(1)
          nodesReceived.length must be_==(3)
          nodesReceived must containAll(updatedNodes)

          mockZooKeeper.getChildren(availabilityNode, true) was called.twice
          mockZooKeeper.getChildren(membershipNode, true) was called.twice          
        }

        "do nothing if not connected" in {
          zooKeeperManager ! NodeChildrenChanged(membershipNode)
          waitFor(10.ms)

          nodesChangedCount must be_==(0)
        }
      }
    }

    "when a Shutdown message is received" in {
      "notify listeners" in {
        zooKeeperManager ! Shutdown
        waitFor(10.ms)

        shutdownCount must be_==(1)
      }

      "shop handling events" in {
        zooKeeperManager ! Shutdown
        zooKeeperManager ! Connected
        waitFor(10.ms)

        shutdownCount must be_==(1)
        connectedCount must be_==(0)
      }
    }

    "when a RemoveNode message is received" in {
      "throw a ClusterDisconnectedException if not connected" in {
        zooKeeperManager !? RemoveNode(1) match {
          case ZooKeeperManagerResponse(r) => r must beSome[Exception].which(_ must haveClass[ClusterDisconnectedException])
        }
      }

      "do nothing if the node does not exist in ZooKeeper" in {
        mockZooKeeper.exists(membershipNode + "/1", false) returns null

        zooKeeperManager ! Connected
        zooKeeperManager !? RemoveNode(1) match {
          case ZooKeeperManagerResponse(r) => r must beNone
        }

        mockZooKeeper.exists(membershipNode + "/1", false) was called
      }

      "remove the znode from ZooKeeper if the node exists" in {
        val path = membershipNode + "/1"

        mockZooKeeper.exists(path, false) returns mock[Stat]
        doNothing.when(mockZooKeeper).delete(path, -1)

        zooKeeperManager ! Connected
        zooKeeperManager !? RemoveNode(1) match {
          case ZooKeeperManagerResponse(r) => r must beNone
        }

        mockZooKeeper.delete(path, -1) was called
      }
    }

    "when a MarkNodeAvailable message is received" in {
      "throw a ClusterDisconnectedException if not connected" in {
        zooKeeperManager !? MarkNodeAvailable(1) match {
          case ZooKeeperManagerResponse(r) => r must beSome[Exception].which(_ must haveClass[ClusterDisconnectedException])
        }
      }

      "add the znode to ZooKeeper if it doesn't exist" in {
        val znodes = List(rootNode, membershipNode, availabilityNode)
        znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])

        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns null
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns path

        zooKeeperManager ! Connected
        zooKeeperManager !? MarkNodeAvailable(1) match {
          case ZooKeeperManagerResponse(r) => r must beNone
        }

        mockZooKeeper.exists(path, false) was called
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) was called
      }

      "do nothing if the znode already exists" in {
        val znodes = List(rootNode, membershipNode, availabilityNode)
        znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])

        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns mock[Stat]
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns path

        zooKeeperManager ! Connected
        zooKeeperManager !? MarkNodeAvailable(1) match {
          case ZooKeeperManagerResponse(r) => r must beNone
        }

        mockZooKeeper.exists(path, false) was called
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) wasnt called        
      }
    }

    "when a MarkNodeUnavailable message is received" in {
      "throw a ClusterDisconnectedException if not connected" in {
        zooKeeperManager !? MarkNodeUnavailable(1) match {
          case ZooKeeperManagerResponse(r) => r must beSome[Exception].which(_ must haveClass[ClusterDisconnectedException])
        }
      }

      "do nothing if the node does not exist in ZooKeeper" in {
        mockZooKeeper.exists(availabilityNode + "/1", false) returns mock[Stat]

        zooKeeperManager ! Connected
        zooKeeperManager !? MarkNodeUnavailable(1) match {
          case ZooKeeperManagerResponse(r) => r must beNone
        }

        mockZooKeeper.exists(availabilityNode + "/1", false) was called
      }

      "remove the znode from ZooKeeper if the node exists" in {
        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns mock[Stat]
        doNothing.when(mockZooKeeper).delete(path, -1)

        zooKeeperManager ! Connected
        zooKeeperManager !? MarkNodeUnavailable(1) match {
          case ZooKeeperManagerResponse(r) => r must beNone
        }

        mockZooKeeper.delete(path, -1) was called
      }
    }
  }

  "ClusterWatcher" should {
    import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}

    var connectedCount = 0
    var disconnectedCount = 0
    var expiredCount = 0
    var nodesChangedCount = 0
    var nodesChangedPath = ""

    val zkm = actor {
      react {
        case Connected => connectedCount += 1
        case Disconnected => disconnectedCount += 1
        case Expired => expiredCount += 1
        case NodeChildrenChanged(path) => nodesChangedCount += 1; nodesChangedPath = path
      }
    }

    val clusterWatcher = new ClusterWatcher(zkm)
    
    def newEvent(state: KeeperState) = {
      val event = mock[WatchedEvent]
      event.getType returns EventType.None
      event.getState returns state

      event
    }

    "send a Connected event when ZooKeeper connects" in {
      val event = newEvent(KeeperState.SyncConnected)

      clusterWatcher.process(event)
      waitFor(10.ms)

      connectedCount must be_==(1)
    }

    "send a Disconnected event when ZooKeeper disconnects" in {
      val event = newEvent(KeeperState.Disconnected)

      clusterWatcher.process(event)
      waitFor(10.ms)

      disconnectedCount must be_==(1)
    }

    "send an Expired event when ZooKeeper's connection expires" in {
      val event = newEvent(KeeperState.Expired)

      clusterWatcher.process(event)
      waitFor(10.ms)

      expiredCount must be_==(1)
    }

    "send a NodeChildrenChanged event when nodes change" in {
      val event = mock[WatchedEvent]
      event.getType returns EventType.NodeChildrenChanged
      val path = "thisIsThePath"
      event.getPath returns path

      clusterWatcher.process(event)
      waitFor(10.ms)

      nodesChangedCount must be_==(1)
      nodesChangedPath must be_==(path)
    }
  }
}
