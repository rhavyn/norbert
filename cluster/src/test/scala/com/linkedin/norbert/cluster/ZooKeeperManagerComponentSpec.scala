package com.linkedin.norbert.cluster

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import actors.Actor
import org.apache.zookeeper.WatchedEvent
import org.specs.util.WaitFor

class ZooKeeperManagerComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with ZooKeeperManagerComponent
        with ClusterNotificationManagerComponent with RouterFactoryComponent {
  import ZooKeeperMessages._

  type Id = Int
  val routerFactory = null

  "ClusterWatcher" should {
    import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}

    var connectedCount = 0
    var disconnectedCount = 0
    var expiredCount = 0
    var nodesChangedCount = 0
    var nodesChangedPath = ""

    val zkm = Actor.actor {
      Actor.react {
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
