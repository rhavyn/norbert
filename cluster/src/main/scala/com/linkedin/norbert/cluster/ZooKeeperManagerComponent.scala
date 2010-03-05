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

import com.linkedin.norbert.util.Logging
import actors.Actor
import Actor._
import java.io.IOException
import org.apache.zookeeper._
import collection.immutable.IntMap

trait ZooKeeperManagerComponent {
  this: ClusterNotificationManagerComponent =>

  sealed trait ZooKeeperManagerMessage
  object ZooKeeperManagerMessages {
    case object Connected extends ZooKeeperManagerMessage
    case object Disconnected extends ZooKeeperManagerMessage
    case object Expired extends ZooKeeperManagerMessage
    case class NodeChildrenChanged(path: String) extends ZooKeeperManagerMessage
    case object Shutdown extends ZooKeeperManagerMessage

    case class RemoveNode(nodeId: Int)
    case class MarkNodeAvailable(nodeId: Int)
    case class MarkNodeUnavailable(nodeId: Int)

    case class ZooKeeperManagerResponse(exception: Option[Exception])
  }

  class ZooKeeperManager(connectString: String, sessionTimeout: Int, clusterName: String, clusterNotificationManager: Actor)
          (implicit zooKeeperFactory: (String, Int, Watcher) => ZooKeeper) extends Actor with Logging {
    private val CLUSTER_NODE = "/" + clusterName
    private val AVAILABILITY_NODE = CLUSTER_NODE + "/available"
    private val MEMBERSHIP_NODE = CLUSTER_NODE + "/members"

    private var zooKeeper: Option[ZooKeeper] = None
    private var watcher: ClusterWatcher = _
    private var connected = false
    private var currentNodes: Map[Int, Node] = IntMap.empty

    def act() = {
      log.ifInfo("Connecting to ZooKeeper...")
      startZooKeeper
      
      while(true) {
        import ZooKeeperManagerMessages._

        receive {
          case Connected => handleConnected

          case Disconnected => handleDisconnected

          case Expired => handleExpired

          case NodeChildrenChanged(path) => if (path == AVAILABILITY_NODE) {
            handleAvailabilityChanged
          } else if (path == MEMBERSHIP_NODE) {
            handleMembershipChanged
          } else {
            log.error("Received a notification for a path that shouldn't be monitored: ", path)
          }

          case RemoveNode(nodeId) => handleRemoveNode(nodeId)

          case MarkNodeAvailable(nodeId) => handleMarkNodeAvailable(nodeId)

          case Shutdown => handleShutdown

          case m => log.error("Received unknown message: %s", m)
        }
      }
    }

    private def handleConnected {
      log.ifDebug("Handling a Connected message")

      if (connected) {
        log.error("Received a Connected message when already connected")
      } else {
        doWithZooKeeper("a Connected message") { zk =>
          verifyZooKeeperStructure(zk)
          lookupCurrentNodes(zk)
          connected = true
          clusterNotificationManager ! ClusterNotificationMessages.Connected(currentNodes)
        }
      }
    }

    private def handleDisconnected {
      log.ifDebug("Handling a Disconnected message")

      doIfConnected("a Disconnected message") {
        connected = false
        currentNodes = IntMap.empty
        clusterNotificationManager ! ClusterNotificationMessages.Disconnected
      }
    }

    private def handleExpired {
      log.ifDebug("Handling an Expired message")

      doIfConnected("an Expired message") {
        log.ifInfo("Connection to ZooKeeper expired, reconnecting...")
        connected = false
        currentNodes = IntMap.empty
        watcher.shutdown
        clusterNotificationManager ! ClusterNotificationMessages.Disconnected
        startZooKeeper
      }
    }

    private def handleAvailabilityChanged {
      log.ifDebug("Handling an availability changed event")

      doIfConnectedWithZooKeeper("an availability changed event") { zk =>
        import scala.collection.jcl.Conversions._

        val available = zk.getChildren(AVAILABILITY_NODE, true)
        currentNodes = available.foldLeft(currentNodes) { case (map, id) =>
          map.get(id.toInt) match {
            case Some(n) if n.available => map
            case Some(n) => map.update(n.id, Node(n.id, n.address, n.partitions, true))
            case None => map
          }
        }

        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
      }
    }

    private def handleMembershipChanged {
      log.ifDebug("Handling a membership changed event")

      doIfConnectedWithZooKeeper("a membership changed event") { zk =>
        lookupCurrentNodes(zk)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
      }
    }

    private def handleRemoveNode(nodeId: Int) {
      log.ifDebug("Handling a RemoveNode(%d) message", nodeId)

      doIfConnectedWithZooKeeperWithResponse("a RemoveNode message", "deleting node") { zk =>
        val path = "%s/%d".format(MEMBERSHIP_NODE, nodeId)

        if (zk.exists(path, false) == null) {
          Some(new InvalidNodeException("Node with id %d does not exist".format(nodeId)))
        } else {
          try {
            zk.delete(path, -1)
            None
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NONODE => None
          }
        }
      }
    }

    private def handleMarkNodeAvailable(nodeId: Int) {
      log.ifDebug("Handling a MarkNodeAvailable(%d) message", nodeId)

      doIfConnectedWithZooKeeperWithResponse("a MarkNodeAvailable message", "marking node available") { zk =>
        val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)

        if (zk.exists(path, false) == null) {
          try {
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
            None
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS => None
          }
        } else {
          None
        }
      }
    }

    private def handleShutdown {
      log.ifDebug("Handling a Shutdown message")

      try {
        zooKeeper.foreach(_.close)
      } catch {
        case ex: Exception => log.error(ex, "Exception when closing connection to ZooKeeper")
      }

      clusterNotificationManager ! ClusterNotificationMessages.Shutdown

      exit
    }

    private def startZooKeeper {
      zooKeeper = try {
        watcher = new ClusterWatcher(self)
        val zk = Some(zooKeeperFactory(connectString, sessionTimeout, watcher))
        log.ifInfo("Connected to ZooKeeper")
        zk
      } catch {
        case ex: IOException =>
          log.error(ex, "Unable to connect to ZooKeeper")
          None

        case ex: Exception =>
          log.error(ex, "Exception while connecting to ZooKeeper")
          None
      }
    }

    private def verifyZooKeeperStructure(zk: ZooKeeper) {
      log.ifDebug("Verifying ZooKeeper structure...")

      List(CLUSTER_NODE, AVAILABILITY_NODE, MEMBERSHIP_NODE).foreach { path =>
        try {
          log.ifDebug("Ensuring %s exists", path)
          if (zk.exists(path, false) == null) {
            log.ifDebug("%s doesn't exist, creating", path)
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          }
        } catch {
          case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS => // do nothing
        }
      }      
    }

    private def lookupCurrentNodes(zk: ZooKeeper) {
      import scala.collection.jcl.Conversions._

      val members = zk.getChildren(MEMBERSHIP_NODE, true)
      val available = zk.getChildren(AVAILABILITY_NODE, true)

      currentNodes = members.foldLeft[Map[Int, Node]](IntMap.empty[Node]) { case (map, member) =>
        val id = member.toInt
        map + (id -> Node(id, zk.getData("%s/%s".format(MEMBERSHIP_NODE, member), false, null), available.contains(member)))
      }
    }

    private def doIfConnected(what: String)(block: => Unit) {
      if (connected) block else log.error("Received %s when not connected", what)
    }

    private def doWithZooKeeper(what: String)(block: ZooKeeper => Unit) {
      zooKeeper match {
        case Some(zk) =>
          try {
            block(zk)
          } catch {
            case ex: KeeperException => log.error(ex, "ZooKeeper threw an exception")
            case ex: Exception => log.error(ex, "Unhandled exception while working with ZooKeeper")
          }

        case None => log.fatal(new Exception,
          "Received %s when ZooKeeper is None, this should never happen. Please report a bug including the stack trace provided.", what)
      }
    }

    private def doIfConnectedWithZooKeeper(what: String)(block: ZooKeeper => Unit) {
      doIfConnected(what) {
        doWithZooKeeper(what)(block)
      }
    }

    private def doIfConnectedWithZooKeeperWithResponse(what: String, exceptionDescription: String)(block: ZooKeeper => Option[Exception]) {
      import ZooKeeperManagerMessages.ZooKeeperManagerResponse

      if (connected) {
        doWithZooKeeper(what) { zk =>
          val response = try {
            block(zk)
          } catch {
            case ex: KeeperException => Some(new InvalidNodeException("Error while %s".format(exceptionDescription), ex))
            case ex: Exception => Some(new InvalidNodeException("Unexpected exception while %s".format(exceptionDescription), ex))
          }

          reply(ZooKeeperManagerResponse(response))
        }
      } else {
        reply(ZooKeeperManagerResponse(Some(new ClusterDisconnectedException("Unable to %s while cluster is disconnected".format(exceptionDescription)))))
      }
    }

    implicit def mapIntNodeToSeqNode(map: Map[Int, Node]): Seq[Node] = map.map { case (key, node) => node }.toSeq
  }

  private implicit def defaultZooKeeperFactory(connectString: String, sessionTimeout: Int, watcher: Watcher) = new ZooKeeper(connectString, sessionTimeout, watcher)
  
  class ClusterWatcher(zooKeeperManager: Actor) extends Watcher {
    @volatile private var shutdownSwitch = false

    def process(event: WatchedEvent) {
      import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
      import ZooKeeperManagerMessages._

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
