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

trait ZooKeeperManagerComponent {
  this: ClusterNotificationManagerComponent =>

  sealed trait ZooKeeperManagerMessage
  object ZooKeeperManagerMessages {
    case object Connected extends ZooKeeperManagerMessage
    case object Disconnected extends ZooKeeperManagerMessage
    case object Expired extends ZooKeeperManagerMessage
    case class NodeChildrenChanged(path: String) extends ZooKeeperManagerMessage
    case object Shutdown extends ZooKeeperManagerMessage
  }

  class ZooKeeperManager(connectString: String, sessionTimeout: Int, clusterName: String, clusterNotificationManager: Actor)
          (implicit zooKeeperFactory: (String, Int, Watcher) => ZooKeeper) extends Actor with Logging {
    private val CLUSTER_NODE = "/" + clusterName
    private val AVAILABILITY_NODE = CLUSTER_NODE + "/available"
    private val MEMBERSHIP_NODE = CLUSTER_NODE + "/members"

    private var zooKeeper: Option[ZooKeeper] = None
    private var currentNodes: Seq[Node] = Nil

    def act() = {
      startZooKeeper
      
      while(true) {
        import ZooKeeperManagerMessages._

        receive {
          case Connected => handleConnected
          case m => log.error("Received unknown message: %s", m)
        }
      }
    }

    private def handleConnected {
      zooKeeper match {
        case Some(zk) =>
          verifyZooKeeperStructure(zk)
          lookupCurrentNodes(zk)
          clusterNotificationManager ! ClusterNotificationMessages.Connected(currentNodes)

        case None =>
          log.error("Received a Connected message when ZooKeeper is None")
      }      
    }

    private def startZooKeeper {
      zooKeeper = try {
        Some(zooKeeperFactory(connectString, sessionTimeout, new ClusterWatcher(self)))
      } catch {
        case ex: IOException =>
          log.error(ex, "Unable to connect to ZooKeeper")
          None

        case ex: Exception =>
          log.fatal(ex, "Exception while connecting to ZooKeeper")
          None
      }
    }

    private def verifyZooKeeperStructure(zk: ZooKeeper) {
      log.ifDebug("Verifying ZooKeeper structure...")

      List(CLUSTER_NODE, AVAILABILITY_NODE, MEMBERSHIP_NODE).foreach { path =>
        log.ifDebug("Ensuring %s exists", path)
        if (zk.exists(path, false) == null) {
          log.ifDebug("%s doesn't exist, creating", path)
          zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        }
      }      
    }

    private def lookupCurrentNodes(zk: ZooKeeper) {
      import scala.collection.jcl.Conversions._

      val members = zk.getChildren(MEMBERSHIP_NODE, true)
      val available = zk.getChildren(AVAILABILITY_NODE, true)

      currentNodes = members.map { member =>
        Node(member.toInt, zk.getData("%s/%s".format(MEMBERSHIP_NODE, member), false, null), available.contains(member))
      }
    }
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
