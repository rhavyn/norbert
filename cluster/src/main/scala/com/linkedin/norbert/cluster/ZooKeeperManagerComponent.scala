package com.linkedin.norbert.cluster

import com.linkedin.norbert.util.Logging
import actors.Actor
import org.apache.zookeeper.{WatchedEvent, ZooKeeper, Watcher}

trait ZooKeeperManagerComponent {
  this: ClusterNotificationManagerComponent =>

  sealed trait ZooKeeperMessage
  object ZooKeeperMessages {
    case object Connected extends ZooKeeperMessage
    case object Disconnected extends ZooKeeperMessage
    case object Expired extends ZooKeeperMessage
    case class NodeChildrenChanged(path: String) extends ZooKeeperMessage
  }

  class ZooKeeperManager(zooKeeperUrls: String, sessionTimeout: Int, clusterName: String)(implicit zooKeeperFactory: (String, Int, Watcher) => ZooKeeper)
          extends Actor with Logging {
    private var zooKeeper: Option[ZooKeeper] = None

    def act() = {
      while(true) {
        import ZooKeeperMessages._

        receive {
          case m => log.error("Received unknown message: %s", m)
        }
      }
    }
  }

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
