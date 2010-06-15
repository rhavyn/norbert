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
import actors.Actor
import Actor._
import cluster.common.{NotificationCenterMessages, ClusterManagerComponent}
import org.apache.zookeeper._
import util.GuardChain

trait ZooKeeperClusterManagerComponent extends ClusterManagerComponent {
  sealed trait ZooKeeperMessage
  object ZooKeeperMessages {
    case object Connected extends ZooKeeperMessage
    case object Disconnected extends ZooKeeperMessage
    case object Expired extends ZooKeeperMessage
    case class NodeChildrenChanged(path: String) extends ZooKeeperMessage
  }

  class ZooKeeperClusterManager(serviceName: String, connectString: String, sessionTimeout: Int,
          zooKeeperFactory: (String, Int, Watcher) => ZooKeeper = defaultZooKeeperFactory) extends BaseClusterManager with Actor with Logging {
    import ClusterManagerMessages._
    import ZooKeeperMessages._

    private val SERVICE_NODE = "/" + serviceName
    private val AVAILABILITY_NODE = SERVICE_NODE + "/available"
    private val MEMBERSHIP_NODE = SERVICE_NODE + "/members"

    private var zooKeeper: ZooKeeper = _
    private var watcher: ClusterWatcher = _
    private var connectedToZooKeeper = false

    def act() = {
      log.debug("ZooKeeperClusterManager started")

      loop {
        react {
          case Connect => handleConnect
          case Connected => handleConnected
          case m @ NodeChildrenChanged(path) if (path == AVAILABILITY_NODE) => logError { ifConnectedToZooKeeper(m) { handleAvailabilityChanged } }
          case m @ NodeChildrenChanged(path) if (path == MEMBERSHIP_NODE) => logError { ifConnectedToZooKeeper(m) { handleMembershipChanged } }
          case m @ Disconnected => logError { ifConnectedToZooKeeper(m) { handleDisconnected } }
          case Expired => handleExpired
          case m @ GetNodes => replyWithError(ifConnected and ifConnectedToZooKeeper(m) then { reply(Nodes(nodes)) })
          case m @ AddNode(node) => replyWithError(ifConnected and ifConnectedToZooKeeper(m) then { handleAddNode(node) })
          case m @ RemoveNode(nodeId) => replyWithError(ifConnected and ifConnectedToZooKeeper(m) then { handleRemoveNode(nodeId) })
          case m @ MarkNodeAvailable(nodeId) => replyWithError(ifConnected and ifConnectedToZooKeeper(m) then { handleMarkNodeAvailable(nodeId) })
          case m @ MarkNodeUnavailable(nodeId) => replyWithError(ifConnected and ifConnectedToZooKeeper(m) then { handleMarkNodeUnavailable(nodeId) })
          case Shutdown => handleShutdown
          case m => log.error("Received invalid message: %s".format(m))
        }
      }
    }

    private def handleConnect {
      if (connected) {
        reply(ClusterManagerResponse(Some(new AlreadyConnectedException)))
      } else {
        try {
          connectToZooKeeper
          reply(ClusterManagerResponse(None))
        } catch {
          case ex: Exception =>
            log.error(ex, "Error connecting to ZooKeeper")
            reply(ClusterManagerResponse(Some(new ClusterException("Unable to connect to ZooKeeper", ex))))
        }
      }
    }
    private def handleConnected {
      log.debug("Handling a ZooKeeper connected event")

      if (connectedToZooKeeper) {
        log.error("Receieved a ZooKeeper connected event while already connected to ZooKeeper")
      } else {
        log.debug("Verifying ZooKeeper structure...")

        List(SERVICE_NODE, AVAILABILITY_NODE, MEMBERSHIP_NODE).foreach { path =>
          try {
            log.debug("Ensuring %s exists".format(path))
            if (zooKeeper.exists(path, false) == null) {
              log.debug("%s doesn't exist, creating".format(path))
              zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
            }
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS => // do nothing
          }
        }

        lookupNodes
        connectedToZooKeeper = true
        notificationCenter ! NotificationCenterMessages.SendConnectedEvent(nodes)
      }
    }

    private def handleAvailabilityChanged {
      log.debug("Handling a ZooKeeper NodesChanged event for the availability node")

      import collection.JavaConversions._

      val availableSet = zooKeeper.getChildren(AVAILABILITY_NODE, true).foldLeft(Set.empty[Int]) { (set, i) => set + i.toInt }
      currentNodes = currentNodes.mapValues { n => n.copy(available = availableSet.contains(n.id)) }

      notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
    }

    private def handleMembershipChanged {
      log.debug("Handling a ZooKeeper NodesChanged event for the membership node")
      lookupNodes
      notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
    }

    private def handleDisconnected {
      log.debug("Handling a ZooKeeper Disconnected event")
      connectedToZooKeeper = false
      currentNodes = Map.empty
      notificationCenter ! NotificationCenterMessages.SendDisconnectedEvent
    }

    private def handleExpired {
      log.debug("Handling a ZooKeeper Expired message")
      log.warn("Connection to ZooKeeper expired, reconnecting")
      connectedToZooKeeper = false
      currentNodes = Map.empty
      watcher.shutdown
      notificationCenter ! NotificationCenterMessages.SendDisconnectedEvent
      try {
        connectToZooKeeper
      } catch {
        case ex: Exception => log.error(ex, "Exception while reconnecting to ZooKeeper")
      }
    }

    private def handleAddNode(node: Node) {
      log.debug("Adding node: %s".format(node))

      val path = "%s/%d".format(MEMBERSHIP_NODE, node.id)
      if (zooKeeper.exists(path, false) != null) throw new InvalidNodeException("A node with id %d already exists".format(node.id))

      try {
        zooKeeper.create(path, node.toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        currentNodes += (node.id -> node)
        notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
        reply(ClusterManagerResponse(None))
      } catch {
        case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS => throw new InvalidNodeException("A node with id %d already exists".format(node.id))
      }
    }

    private def handleRemoveNode(nodeId: Int) {
      log.debug("Removing node with id: %d".format(nodeId))

      val path = "%s/%d".format(MEMBERSHIP_NODE, nodeId)
      if (zooKeeper.exists(path, false) != null) {
        try {
          zooKeeper.delete(path, -1)
        } catch {
          case ex: KeeperException if ex.code == KeeperException.Code.NONODE =>
        }

        currentNodes -= nodeId
        notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
      }

      reply(ClusterManagerResponse(None))
    }

    private def handleMarkNodeAvailable(nodeId: Int) {
      log.debug("Marking node with id %d available".format(nodeId))

      val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)
      if (zooKeeper.exists(path, false) == null) {
        try {
          zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        } catch {
          case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS =>
        }

        currentNodes.get(nodeId).foreach { n => currentNodes += (n.id -> n.copy(available = true)) }
        notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
      }

      reply(ClusterManagerResponse(None))
    }

    private def handleMarkNodeUnavailable(nodeId: Int) {
      log.debug("Marking node with id %d unavailable".format(nodeId))

      val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)
      if (zooKeeper.exists(path, false) != null) {
        try {
          zooKeeper.delete(path, -1)
        } catch {
          case ex: KeeperException if ex.code == KeeperException.Code.NONODE =>
        }

        currentNodes.get(nodeId).foreach { n => currentNodes += (n.id -> n.copy(available = false)) }
        notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
      }

      reply(ClusterManagerResponse(None))
    }

    private def handleShutdown {
      log.debug("Shutting down ZooKeeperClusterManager")

      if (watcher != null) watcher.shutdown
      try {
        if (zooKeeper != null) zooKeeper.close
      } catch {
        case ex: Exception => log.error(ex, "Exception which closing connection to ZooKeeper")
      }

      reply(Shutdown)

      log.debug("ZooKeeperClusterManager shutdown")
      exit
    }

    private def connectToZooKeeper {
      log.debug("Connecting to ZooKeeper...")

      watcher = new ClusterWatcher(self)
      zooKeeper = zooKeeperFactory(connectString, sessionTimeout, watcher)
      connected = true
      log.debug("Connected to ZooKeeper")
    }

    private def lookupNodes {
      import collection.JavaConversions._

      val members = zooKeeper.getChildren(MEMBERSHIP_NODE, true)
      val available = zooKeeper.getChildren(AVAILABILITY_NODE, true)

      currentNodes = members.foldLeft(Map.empty[Int, Node]) { (map, id) =>
        map + (id.toInt -> Node(zooKeeper.getData("%s/%s".format(MEMBERSHIP_NODE, id), false, null), available.contains(id)))
      }
    }

    private def ifConnectedToZooKeeper(msg: Any) = GuardChain[Unit](connectedToZooKeeper,
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

  protected implicit def defaultZooKeeperFactory(connectString: String, sessionTimeout: Int, watcher: Watcher) = new ZooKeeper(connectString, sessionTimeout, watcher)

  class ClusterWatcher(zooKeeperManager: Actor) extends Watcher {
    @volatile private var shutdownSwitch = false

    def process(event: WatchedEvent) {
      import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
      import ZooKeeperMessages._

      if (shutdownSwitch) return

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
}
