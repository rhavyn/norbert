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
    var nodesReceived: Seq[Node] = Nil

    val notificationActor = actor {
      react {
        case ClusterNotificationMessages.Connected(nodes) => connectedCount += 1; nodesReceived = nodes
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

        connectedCount must be_==(1)
        nodesReceived must haveTheSameElementsAs(nodes)
      }
    }

    "when a Disconnected message is received" in {

    }

    "when an Expired message is received" in {

    }

    "when a NodeChildrenChanged message is received" in {

    }

    "when a Shutdown message is received" in {
      
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
