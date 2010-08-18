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

import logging.Logging
import actors.{Actor, DaemonActor}
import Actor._
import util.GuardChain
import org.apache.zookeeper._
import cluster.common._

object ZooKeeperClusterManager {
  private def defaultZooKeeperFactory(connectString: String, sessionTimeout: Int, watcher: Watcher) = new ZooKeeper(connectString, sessionTimeout, watcher)
}

class ZooKeeperClusterManager(protected val delegate: ClusterManagerDelegate, serviceName: String, connectString: String, sessionTimeout: Int,
        zooKeeperFactory: (String, Int, Watcher) => ZooKeeper = ZooKeeperClusterManager.defaultZooKeeperFactory) extends ClusterManager {
  private val SERVICE_ZNODE_PATH = "/" + serviceName
  private val AVAILABILITY_ZNODE_PATH = SERVICE_ZNODE_PATH + "/available"
  private val MEMBERSHIP_ZNODE_PATH = SERVICE_ZNODE_PATH + "/members"

  private var zooKeeper: ZooKeeper = _
  private var watcher: ClusterWatcher = _
  private var connectedToZooKeeper = false

  def act() = {
    watcher = new ClusterWatcher(self)
    connectToZooKeeper

    loop {
      import ZooKeeperMessages._
      import ClusterManagerMessages._

      react {
        case Connected => handleConnected
        case m @ NodeChildrenChanged(path) if (path == AVAILABILITY_ZNODE_PATH) => logError { ifConnectedToZooKeeper(m) then { handleAvailabilityChanged } }
        case m @ NodeChildrenChanged(path) if (path == MEMBERSHIP_ZNODE_PATH) => logError { ifConnectedToZooKeeper(m) then { handleMembershipChanged } }
        case m @ Disconnected => logError { ifConnectedToZooKeeper(m) then { handleDisconnected } }
        case Expired => handleExpired
        case m @ AddNode(node) => replyWithError(ifConnectedToZooKeeper(m) then { handleAddNode(node) })
        case m @ RemoveNode(nodeId) => replyWithError(ifConnectedToZooKeeper(m) then { handleRemoveNode(nodeId) })
        case m @ MarkNodeAvailable(nodeId) => replyWithError(ifConnectedToZooKeeper(m) then { handleMarkNodeAvailable(nodeId) })
        case m @ MarkNodeUnavailable(nodeId) => replyWithError(ifConnectedToZooKeeper(m) then { handleMarkNodeUnavailable(nodeId) })
        case Shutdown =>
          watcher.shutdown
          zooKeeper.close
          invokeDelegate(delegate.didShutdown)
          exit
      }
    }
  }

  private def connectToZooKeeper {
    log.debug("Connecting to ZooKeeper...")
    try {
      zooKeeper = zooKeeperFactory(connectString, sessionTimeout, watcher)
    } catch {
      case ex: Exception =>
        invokeDelegate(delegate.connectionFailed(new ClusterException(ex)))
        exit
    }
  }

  private def handleConnected {
    log.debug("Handling a ZooKeeper connected event")

    if (connectedToZooKeeper) {
      log.error("Receieved a ZooKeeper connected event while already connected to ZooKeeper")
    } else {
      log.debug("Verifying ZooKeeper structure...")

      List(SERVICE_ZNODE_PATH, AVAILABILITY_ZNODE_PATH, MEMBERSHIP_ZNODE_PATH).foreach { path =>
        try {
          log.debug("Ensuring %s exists".format(path))
          if (zooKeeper.exists(path, false) == null) {
            log.debug("%s doesn't exist, creating".format(path))
            zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          }
        } catch {
          case ex: KeeperException.NodeExistsException => // do nothing

          case ex: Exception =>
            invokeDelegate(delegate.connectionFailed(new ClusterException(ex)))
            exit
        }
      }

      logError(currentNodes = lookupNodes)
      connectedToZooKeeper = true
      invokeDelegate(delegate.didConnect(nodeSet))
    }
  }

  private def handleAvailabilityChanged {
    log.debug("Handling a ZooKeeper NodesChanged event for the availability node")

    import collection.JavaConversions._

    availableNodeIds = zooKeeper.getChildren(AVAILABILITY_ZNODE_PATH, true).foldLeft(Set.empty[Int]) { (set, i) => set + i.toInt }
    updateCurrentNodesAndNotifyDelegate(currentNodes.mapValues { n => n.copy(available = availableNodeIds.contains(n.id)) })
  }

  private def handleMembershipChanged {
    log.debug("Handling a ZooKeeper NodesChanged event for the membership node")
    updateCurrentNodesAndNotifyDelegate(lookupNodes)
  }

  private def handleDisconnected {
    log.debug("Handling a ZooKeeper Disconnected event")
    connectedToZooKeeper = false
    currentNodes = Map.empty
    invokeDelegate(delegate.didDisconnect)
  }

  private def handleExpired {
    log.debug("Handling a ZooKeeper Expired message")
    log.warn("Connection to ZooKeeper expired, reconnecting")
    connectedToZooKeeper = false
    currentNodes = Map.empty
    invokeDelegate(delegate.didDisconnect)
    connectToZooKeeper
  }

  import ClusterManagerMessages.ClusterManagerResponse

  private def handleAddNode(node: Node) {
    val n = node.copy(available = availableNodeIds.contains(node.id))
    log.debug("Adding node: %s".format(n))

    val path = "%s/%d".format(MEMBERSHIP_ZNODE_PATH, n.id)
    if (zooKeeper.exists(path, false) != null) throw new InvalidNodeException("A node with id %d already exists".format(n.id))

    try {
      zooKeeper.create(path, n.toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      currentNodes += (n.id -> n)
      invokeDelegate(delegate.nodesDidChange(nodeSet))
      reply(ClusterManagerResponse(None))
    } catch {
      case ex: KeeperException.NodeExistsException => throw new InvalidNodeException("A node with id %d already exists".format(n.id))
    }
  }

  private def handleRemoveNode(nodeId: Int) {
    log.debug("Removing node with id: %d".format(nodeId))

    val path = "%s/%d".format(MEMBERSHIP_ZNODE_PATH, nodeId)
    if (zooKeeper.exists(path, false) != null) {
      try {
        zooKeeper.delete(path, -1)
      } catch {
        case ex: KeeperException.NoNodeException =>
      }

      currentNodes -= nodeId
      invokeDelegate(delegate.nodesDidChange(nodeSet))
    }

    reply(ClusterManagerResponse(None))
  }

  private def handleMarkNodeAvailable(nodeId: Int) {
    if (!availableNodeIds.contains(nodeId)) {
      log.debug("Marking node with id %d available".format(nodeId))

      val path = "%s/%d".format(AVAILABILITY_ZNODE_PATH, nodeId)
      val updated = if (zooKeeper.exists(path, false) == null) {
        try {
          zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          true
        } catch {
          case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS => false
        }
      } else {
        false
      }

      availableNodeIds += nodeId
      if (updated) if (setNodeWithIdAvailabilityTo(nodeId, true)) invokeDelegate(delegate.nodesDidChange(nodeSet))
    }

    reply(ClusterManagerResponse(None))
  }

  private def handleMarkNodeUnavailable(nodeId: Int) {
    if (availableNodeIds.contains(nodeId)) {
      log.debug("Marking node with id %d unavailable".format(nodeId))

      val path = "%s/%d".format(AVAILABILITY_ZNODE_PATH, nodeId)
      val updated = if (zooKeeper.exists(path, false) != null) {
        try {
          zooKeeper.delete(path, -1)
          true
        } catch {
          case ex: KeeperException if ex.code == KeeperException.Code.NONODE => false
        }
      } else {
        false
      }

      availableNodeIds -= nodeId
      if (updated) if (setNodeWithIdAvailabilityTo(nodeId, false)) invokeDelegate(delegate.nodesDidChange(nodeSet))
    }

    reply(ClusterManagerResponse(None))
  }

  private def lookupNodes = {
    import collection.JavaConversions._

    val members = zooKeeper.getChildren(MEMBERSHIP_ZNODE_PATH, true)
    val available = zooKeeper.getChildren(AVAILABILITY_ZNODE_PATH, true)

    members.foldLeft(Map.empty[Int, Node]) { (map, id) =>
      map + (id.toInt -> Node(zooKeeper.getData("%s/%s".format(MEMBERSHIP_ZNODE_PATH, id), false, null), available.contains(id)))
    }
  }

  private def updateCurrentNodesAndNotifyDelegate(op: => Map[Int, Node]) {
    val ns = op
    if (currentNodes != ns) {
      currentNodes = ns
      invokeDelegate(delegate.nodesDidChange(nodeSet))
    }
  }

  private def ifConnectedToZooKeeper(msg: Any) = GuardChain(connectedToZooKeeper,
    throw new ClusterDisconnectedException("Received message while not connected to ZooKeeper: %s".format(msg)))

  private def logError[A](op: => A) {
    try {
      op
    } catch {
      case ex: ClusterException => log.error(ex, "Error processing message")
      case ex: KeeperException => log.error(ex, "ZooKeeper threw an exception")
      case ex: InterruptedException => log.error(ex, "Interrupted while processing message")
    }
  }

  private def replyWithError[A](op: => A) {
    val o = try {
      op
      None
    } catch {
      case ex: ClusterException => Some(ex)
      case ex: Exception => Some(new ClusterException("Error processing message", ex))
    }

    reply(ClusterManagerResponse(o))
  }
}

private[zookeeper] class ClusterWatcher(zooKeeperManager: Actor) extends Watcher {
  @volatile private var shutdownSwitch = false

  def process(event: WatchedEvent) {
    import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}

    if (shutdownSwitch) return

    import ZooKeeperMessages._
    event.getType match {
      case EventType.None =>
        event.getState match {
          case KeeperState.SyncConnected => zooKeeperManager ! Connected
          case KeeperState.Disconnected => zooKeeperManager ! Disconnected
          case KeeperState.Expired => zooKeeperManager ! Expired
        }

      case EventType.NodeChildrenChanged => zooKeeperManager ! NodeChildrenChanged(event.getPath)
    }
  }

  def shutdown: Unit = shutdownSwitch = true
}

private object ZooKeeperMessages {
  case object Connected
  case object Disconnected
  case object Expired
  case class NodeChildrenChanged(path: String)
}
