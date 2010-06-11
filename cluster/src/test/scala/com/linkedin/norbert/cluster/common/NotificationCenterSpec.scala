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
package common

import org.specs.Specification
import org.specs.util.WaitFor

object NotificationCenterSpec extends Specification with WaitFor {
  "NotificationCenter" should {
    val notificationCenter = new NotificationCenter
    notificationCenter.start
    import NotificationCenterMessages._

    doAfter { notificationCenter ! SendShutdownEvent }

    "when AddListener is called" in {
      "return a ClusterListenerKey" in {
        notificationCenter !? (1000, AddListener(ClusterListener {
          case ClusterEvents.Shutdown =>
        })) must beSomething.which { _ must haveClass[AddedListener] }
      }

      "send a Connected event if the cluster is connected" in {
        var callCount1 = 0
        notificationCenter !? (1000, AddListener(ClusterListener {
          case ClusterEvents.Connected(_) => callCount1 += 1
        }))

        waitFor(20.ms)
        callCount1 must be_==(0)

        notificationCenter ! SendConnectedEvent(Set.empty)

        var callCount2 = 0
        notificationCenter !? (1000, AddListener(ClusterListener {
          case ClusterEvents.Connected(_) => callCount2 += 1
        }))

        callCount2 must eventually(be_==(1))
      }
    }

    "when a RemoveListener message is received remove the listener" in {
      var callCount = 0
      val key = notificationCenter !? AddListener(ClusterListener {
        case ClusterEvents.Connected(_) => callCount += 1
        case ClusterEvents.NodesChanged(_) => callCount += 1
      }) match {
        case AddedListener(key) => key
      }

      notificationCenter ! SendConnectedEvent(Set.empty)

      callCount must eventually(be_==(1))

      notificationCenter ! RemoveListener(key)

      notificationCenter ! SendNodesChangedEvent(Set.empty)

      waitFor(20.ms)
      callCount must be_==(1)
    }

    "when a Connected message is received" in {
      "send a Connected event to registered listeners containing available nodes" in {
        var callCount = 0
        var nodes: Set[Node] = null
        notificationCenter ! AddListener(ClusterListener {
          case ClusterEvents.Connected(n) =>
            callCount += 1
            nodes = n
        })

        notificationCenter ! SendConnectedEvent(Set(Node(1, "localhost: 31313", false), Node(2, "localhost: 31313", true)))
        callCount must eventually(be_==(1))
        nodes must haveSize(1)
        nodes.head.id must be_==(2)
      }

      "do nothing if already connected" in {
        var callCount = 0
        notificationCenter ! AddListener(ClusterListener {
          case ClusterEvents.Connected(n) => callCount += 1
        })

        notificationCenter ! SendConnectedEvent(Set.empty)
        notificationCenter ! SendConnectedEvent(Set.empty)
        waitFor(20.ms)
        callCount must be_==(1)
      }
    }

    "when a NodesChanged message is received" in {
      "send a NodesChanged event to registered listeners containing available nodes" in {
        var callCount = 0
        var nodes: Set[Node] = null
        notificationCenter ! AddListener(ClusterListener {
          case ClusterEvents.NodesChanged(n) =>
            callCount += 1
            nodes = n
        })

        notificationCenter ! SendConnectedEvent(Set.empty)
        notificationCenter ! SendNodesChangedEvent(Set(Node(1, "localhost: 31313", false), Node(2, "localhost: 31313", true)))
        callCount must eventually(be_==(1))
        nodes must haveSize(1)
        nodes.head.id must be_==(2)
      }

      "do nothing if not connected" in {
        var callCount = 0
        notificationCenter ! AddListener(ClusterListener {
          case ClusterEvents.NodesChanged(n) => callCount += 1
        })

        notificationCenter ! SendNodesChangedEvent(Set.empty)
        waitFor(20.ms)
        callCount must be_==(0)
      }
    }

    "when a Disconnected message is received" in {
      "notify listeners that the cluster is disconnected" in {
        var callCount = 0
        notificationCenter ! AddListener(ClusterListener {
          case ClusterEvents.Disconnected => callCount += 1
        })

        notificationCenter ! SendConnectedEvent(Set.empty)
        notificationCenter ! SendDisconnectedEvent

        callCount must eventually(be_==(1))
      }

      "do nothing if already disconnected" in {
        var callCount = 0
        notificationCenter ! AddListener(ClusterListener {
          case ClusterEvents.Disconnected => callCount += 1
        })

        notificationCenter ! SendConnectedEvent(Set.empty)
        notificationCenter ! SendDisconnectedEvent
        notificationCenter ! SendDisconnectedEvent

        waitFor(20.ms)
        callCount must be_==(1)
      }
    }

    "when a Shutdown message is received" in {
      "send a Shutdown event to all listeners" in {
        var callCount = 0
        notificationCenter ! AddListener(ClusterListener {
          case ClusterEvents.Shutdown => callCount += 1
        })

        notificationCenter ! SendShutdownEvent
        callCount must eventually(be_==(1))
      }

      "stop responding to further messages" in {
        var callCount = 0
        notificationCenter ! AddListener(ClusterListener {
          case ClusterEvents.Connected(_) => callCount += 1
          case ClusterEvents.Shutdown => callCount += 1
        })

        notificationCenter ! SendShutdownEvent
        notificationCenter ! SendConnectedEvent(Set.empty)

        waitFor(20.ms)
        callCount must be_==(1)
      }
    }

    "handle a ClusterListener that throws an exception" in {
      var callCount = 0
      notificationCenter ! AddListener(ClusterListener {
        case ClusterEvents.NodesChanged(_) =>
          callCount += 1
          throw new Exception
      })

      notificationCenter ! SendConnectedEvent(Set.empty)
      notificationCenter ! SendNodesChangedEvent(Set.empty)
      notificationCenter ! SendNodesChangedEvent(Set.empty)

      callCount must eventually(be_==(2))
    }
  }
}
