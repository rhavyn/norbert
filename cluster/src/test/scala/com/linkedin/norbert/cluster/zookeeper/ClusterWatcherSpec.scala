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

import org.specs.Specification
import actors.Actor._
import org.apache.zookeeper.WatchedEvent
import org.specs.mock.Mockito

class ClusterWatcherSpec extends Specification with Mockito {
  import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
  import ZooKeeperMessages._

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
      case 'quit => exit
    }
  }

  val clusterWatcher = new ClusterWatcher(zkm)

  "ClusterWatcher" should {
    doAfter { zkm ! 'quit }

    def newEvent(state: KeeperState) = {
      val event = mock[WatchedEvent]
      event.getType returns EventType.None
      event.getState returns state

      event
    }

    "send a Connected event when ZooKeeper connects" in {
      val event = newEvent(KeeperState.SyncConnected)

      clusterWatcher.process(event)

      connectedCount must eventually(be_==(1))
    }

    "send a Disconnected event when ZooKeeper disconnects" in {
      val event = newEvent(KeeperState.Disconnected)

      clusterWatcher.process(event)

      disconnectedCount must eventually(be_==(1))
    }

    "send an Expired event when ZooKeeper's connection expires" in {
      val event = newEvent(KeeperState.Expired)

      clusterWatcher.process(event)

      expiredCount must eventually(be_==(1))
    }

    "send a NodeChildrenChanged event when nodes change" in {
      val event = mock[WatchedEvent]
      event.getType returns EventType.NodeChildrenChanged
      val path = "thisIsThePath"
      event.getPath returns path

      clusterWatcher.process(event)

      nodesChangedCount must eventually(be_==(1))
      nodesChangedPath must be_==(path)
    }
  }
}
