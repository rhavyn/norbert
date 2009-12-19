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

import java.net.InetSocketAddress
import org.apache.zookeeper._
import com.linkedin.norbert.util.Logging

/**
 * A component which manages the connection to ZooKeeper.
 */
trait ZooKeeperMonitorComponent {
  this: ClusterWatcherComponent =>

  val zooKeeperMonitor: ZooKeeperMonitor

  class ZooKeeperMonitor(zooKeeperUrls: String, sessionTimeout: Int, clusterName: String)(implicit zooKeeperFactory: (String, Int, Watcher) => ZooKeeper) extends Logging {
    private val CLUSTER_NODE = "/" + clusterName
    private val AVAILABILITY_NODE = CLUSTER_NODE + "/available"
    private val MEMBERSHIP_NODE = CLUSTER_NODE + "/members"

    @volatile private var zookeeper: Option[ZooKeeper] = None

    def start(): Unit = {
      log.ifInfo("ZooKeeperMonitor started, connecting to ZooKeeper at %s with a session timeout of %ds...", zooKeeperUrls, sessionTimeout)
      zookeeper = Some(zooKeeperFactory(zooKeeperUrls, sessionTimeout, clusterWatcher))
    }
    
    def verifyStructure: Unit = doWithZooKeeper { zk =>
      log.info("Verifying ZooKeeper structure...")

      List(CLUSTER_NODE, AVAILABILITY_NODE, MEMBERSHIP_NODE).foreach { path =>
        log.info("Ensuring %s exists", path)
        if (zk.exists(path, false) == null) {
          log.info("%s doesn't exist, creating", path)
          zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        }
      }
    }

    def currentNodes: Seq[Node] = doWithZooKeeper { zk =>
      import scala.collection.jcl.Conversions._

      val members = zk.getChildren(MEMBERSHIP_NODE, true)
      val available = zk.getChildren(AVAILABILITY_NODE, true)

      members.map { member =>
        Node(member.toInt, zk.getData("%s/%s".format(MEMBERSHIP_NODE, member), false, null), available.contains(member))
      }
    }

    def addNode(nodeId: Int, address: InetSocketAddress, partitions: Array[Int]): Node = doWithZooKeeper { zk =>
      val path = "%s/%d".format(MEMBERSHIP_NODE, nodeId)
      val node = Node(nodeId, address, partitions, false)

      try {
        if (zk.exists(path, false) == null) {
          zk.create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          node
        } else {
          throw new InvalidNodeException("Node with id %d already exists".format(nodeId))
        }
      } catch {
        case ex: KeeperException => throw new InvalidNodeException("Error while creating node", ex)
      }
    }

    def removeNode(nodeId: Int): Unit = doWithZooKeeper { zk =>
      val path = "%s/%d".format(MEMBERSHIP_NODE, nodeId)

      try {
        if (zk.exists(path, false) == null) throw new InvalidNodeException("Node with id %d does not exist".format(nodeId))
        else zk.delete(path, -1)
      }
      catch {
        case ex: KeeperException => throw new InvalidNodeException("Error while deleting node", ex)
      }
    }
    
    def markNodeAvailable(nodeId: Int): Unit = doWithZooKeeper { zk =>
      val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)
      if (zk.exists(path, false) == null) {
        zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      }
    }

    def shutdown(): Unit = doWithZooKeeper { zk =>
      log.info("Shutting down ZooKeeperMonitor")
      close(zk)
    }
    
    def reconnect: Unit = {
      doWithZooKeeper(close(_))
      start
    }

    private def close(zk: ZooKeeper): Unit = {
      zookeeper = None
      zk.close
    }

    private def doWithZooKeeper[T](f: ZooKeeper => T): T = {
      try {
        f(zookeeper getOrElse (throw new ClusterDisconnectedException("You must call start before calling methods on ZooKeeperMonitor")))
      }
      catch {
        case ex: KeeperException => throw new ClusterException("Error performing ZooKeeper operation", ex)
      }
    }
  }

  implicit def zkCreator(zooKeeperUrls: String, sessionTimeout: Int, watcher: Watcher) = new ZooKeeper(zooKeeperUrls, sessionTimeout, watcher)
}
