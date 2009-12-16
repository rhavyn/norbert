/*
 * Copyright 2009 LinkedIn, Inc
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
import org.apache.zookeeper.{CreateMode, ZooDefs, Watcher, ZooKeeper}
import org.apache.zookeeper.data.Stat
import org.specs.mock.Mockito
import java.util.ArrayList
import java.net.InetSocketAddress

class ZooKeeperMonitorComponentSpec extends SpecificationWithJUnit with Mockito
        with ZooKeeperMonitorComponent with ClusterWatcherComponent with ClusterManagerComponent
        with RouterFactoryComponent with ClusterComponent {
  val clusterName = "test"
  val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(mockZkCreator _)
  val clusterWatcher = null
  val clusterManager = null
  val routerFactory = null
  val cluster = null
  
  val mockZooKeeper = mock[ZooKeeper]
  def mockZkCreator(zooKeeperUrls: String, sessionTimeout: Int, watcher: Watcher) = mockZooKeeper

  zooKeeperMonitor.start

  val rootNode = "/test"
  val membershipNode = rootNode + "/members"
  val availabilityNode = rootNode + "/available"

  "ZooKeeperMonitor" should {
    "when verifying ZooKeeper structure" in {
      val znodes = List(rootNode, membershipNode, availabilityNode)

      "throw ClusterDisconnectedException if ZooKeeperMonitor has not been started" in {
        val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(mockZkCreator _)
        zooKeeperMonitor.verifyStructure must throwA[ClusterDisconnectedException]
      }

      "do nothing if all znodes exist" in {
        znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])

        zooKeeperMonitor.verifyStructure

        znodes.foreach(mockZooKeeper.exists(_, false) was called)
      }

      "create the cluster, membership and availability znodes if they do not already exist" in {
        znodes.foreach { path =>
          mockZooKeeper.exists(path, false) returns null
          mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns path
        }

        zooKeeperMonitor.verifyStructure

        znodes.foreach { path =>
          mockZooKeeper.exists(path, false) was called
          mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) was called
        }
      }
    }

    "when calculating current nodes" in {
      "throw ClusterDisconnectedException if ZooKeeperMonitor has not been started" in {
        val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(mockZkCreator _)
        zooKeeperMonitor.currentNodes must throwA[ClusterDisconnectedException]
      }

      "return the current list of nodes" in {
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

        zooKeeperMonitor.currentNodes must haveTheSameElementsAs(nodes)

        mockZooKeeper.getChildren(membershipNode, true) was called
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) was called
        }
        mockZooKeeper.getChildren(availabilityNode, true) was called
      }
    }

    "when adding a new node" in {
      "throw ClusterDisconnectedException if ZooKeeperMonitor has not been started" in {
        val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(mockZkCreator _)
        zooKeeperMonitor.addNode(1, new InetSocketAddress("localhost", 31313), Array(1, 2)) must throwA[ClusterDisconnectedException]
      }

      "throw an InvalidNodeException if the node already exists in ZooKeeper" in {
        mockZooKeeper.exists(membershipNode + "/1", false) returns mock[Stat]

        zooKeeperMonitor.addNode(1, new InetSocketAddress("localhost", 31313), Array(1, 2)) must throwA[InvalidNodeException]
        
        mockZooKeeper.exists(membershipNode + "/1", false) was called
      }

      "add a znode to ZooKeeper if the node doesn't already exist" in {
        val node = Node(1, new InetSocketAddress("localhost", 31313), Array(1, 2), false)
        val path = membershipNode + "/1"

        mockZooKeeper.exists(path, false) returns null
        mockZooKeeper.create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns path

        val newNode = zooKeeperMonitor.addNode(1, new InetSocketAddress("localhost", 31313), Array(1, 2))

        newNode.id must be_==(node.id)
        newNode.address must be_==(node.address)
        newNode.partitions must containInOrder(node.partitions)

        mockZooKeeper.create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) was called
      }
    }

    "when removing a new node" in {
      "throw ClusterDisconnectedException if ZooKeeperMonitor has not been started" in {
        val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(mockZkCreator _)
        zooKeeperMonitor.removeNode(1) must throwA[ClusterDisconnectedException]
      }

      "throw an InvalidNodeException if the node does not exist in ZooKeeper" in {
        mockZooKeeper.exists(membershipNode + "/1", false) returns null

        zooKeeperMonitor.removeNode(1) must throwA[InvalidNodeException]

        mockZooKeeper.exists(membershipNode + "/1", false) was called
      }

      "remove the znode from ZooKeeper if the node exists" in {
        val path = membershipNode + "/1"

        mockZooKeeper.exists(path, false) returns mock[Stat]
        doNothing.when(mockZooKeeper).delete(path, -1)

        zooKeeperMonitor.removeNode(1)

        mockZooKeeper.delete(path, -1) was called
      }
    }

    "when marking a node available" in {
      "throw ClusterDisconnectedException if ZooKeeperMonitor has not been started" in {
        val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(mockZkCreator _)
        zooKeeperMonitor.markNodeAvailable(1) must throwA[ClusterDisconnectedException]
      }

      "add a znode to ZooKeeper if the node doesn't exist" in {
        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns null
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns path

        zooKeeperMonitor.markNodeAvailable(1)
        
        mockZooKeeper.exists(path, false) was called
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) was called
      }

      "not add a znode to ZooKeeper if the node does exist" in {
        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns mock[Stat]
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns path

        zooKeeperMonitor.markNodeAvailable(1)

        mockZooKeeper.exists(path, false) was called
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) wasnt called
      }
    }

    "when shutting down" in {
      "throw ClusterDisconnectedException if ZooKeeperMonitor has not been started" in {
        val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(mockZkCreator _)
        zooKeeperMonitor.shutdown must throwA[ClusterDisconnectedException]
      }

      "properly close the connection to ZooKeeper" in {
        doNothing.when(mockZooKeeper).close()

        zooKeeperMonitor.shutdown

        mockZooKeeper.close was called
      }
    }

    "when restarting" in {
      "throw ClusterDisconnectedException if ZooKeeperMonitor has not been started" in {
        val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(mockZkCreator _)
        zooKeeperMonitor.reconnect must throwA[ClusterDisconnectedException]
      }

      "close and then open a new ZooKeeper instance" in {
        var callCount = 0
        def countZooKeeperCreator(zooKeeperUrls: String, sessionTimeout: Int, watcher: Watcher) = {
          callCount += 1
          mockZooKeeper
        }

        val zooKeeperMonitor = new ZooKeeperMonitor("urls", 1, clusterName)(countZooKeeperCreator _)
        zooKeeperMonitor.start  // callCount += 1
        doNothing.when(mockZooKeeper).close()

        zooKeeperMonitor.reconnect  // callCount += 1

        mockZooKeeper.close was called
        callCount must be_==(2)
      }
    }
  }
}
