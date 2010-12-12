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
import org.specs.mock.Mockito
import org.specs.util.WaitFor
import actors.Actor._

class ClusterNotificationManagerComponentSpec extends Specification with Mockito with WaitFor with ClusterNotificationManagerComponent {
  val clusterNotificationManager = new ClusterNotificationManager

  clusterNotificationManager.start

  val shortNodes = Set(Node(1, "localhost:31313", false, Set(1, 2)))
  val nodes = shortNodes ++ List(Node(2, "localhost:31314", true, Set(3, 4)),
    Node(3, "localhost:31315", false, Set(5, 6)))

  "ClusterNotificationManager" should {
    import ClusterNotificationMessages._

    "when handling an AddListener message" in {
      "send a Connected event to the listener if the cluster is connected" in {
        clusterNotificationManager ! Connected(nodes)

        var callCount = 0
        var currentNodes: Set[Node] = Set()
        val listener = actor {
          react {
            case ClusterEvents.Connected(n) => callCount += 1; currentNodes = n
          }
        }

        clusterNotificationManager ! AddListener(listener)
        callCount must eventually(be_==(1))
        currentNodes.size must be_==(1)
        currentNodes.foreach { node =>
          node.id must be_==(2)
        }
      }

      "not send a Connected event to the listener if the cluster is not connected" in {
        var callCount = 0
        val listener = actor {
          react {
            case ClusterEvents.Connected(_) => callCount += 1
          }
        }

        clusterNotificationManager ! AddListener(listener)
        waitFor(20.ms)
        callCount must be_==(0)
      }
    }

    "when handling a RemoveListener message remove the listener" in {
      var callCount = 0
      val listener = actor {
        loop {
          react {
            case ClusterEvents.Connected(_) => callCount += 1
            case ClusterEvents.NodesChanged(_) => callCount += 1
          }
        }
      }

      val key = clusterNotificationManager !? AddListener(listener) match {
        case AddedListener(key) => key
      }

      clusterNotificationManager ! Connected(nodes)
      clusterNotificationManager ! RemoveListener(key)
      clusterNotificationManager ! NodesChanged(nodes)

      callCount must eventually(be_==(1))
    }

    "when handling a Connected message" in {
      "notify listeners" in {
        var callCount = 0
        var currentNodes: Set[Node] = Set()
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Connected(n) => callCount += 1; currentNodes = n
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Connected(nodes)

        callCount must eventually(be_==(1))
        currentNodes.size must be_==(1)
        currentNodes.foreach { node =>
          node.id must be_==(2)
        }
      }

      "do nothing if already connected" in {
        var callCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Connected(_) => callCount += 1
              case _ =>
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Connected(nodes)
        clusterNotificationManager ! Connected(nodes)

        callCount must eventually(be_==(1))
      }
    }

    "when handling a NodesChanged message" in {
      "notify listeners" in {
        var callCount = 0
        var currentNodes: Set[Node] = Set()
        val listener = actor {
          loop {
            react {
              case ClusterEvents.NodesChanged(n) =>
                callCount += 1
                currentNodes = n
            }
          }
        }

        clusterNotificationManager ! Connected(shortNodes)
        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! NodesChanged(nodes)

        callCount must eventually(be_==(1))
        currentNodes.size must be_==(1)
        currentNodes.foreach { node =>
          node.id must be_==(2)
        }
      }
    }

    "do nothing is not connected" in {
      var callCount = 0
      val listener = actor {
        loop {
          react {
            case ClusterEvents.NodesChanged(n) => callCount += 1
            case _ =>
          }
        }
      }

      clusterNotificationManager ! Connected(shortNodes)
      clusterNotificationManager ! AddListener(listener)
      clusterNotificationManager ! NodesChanged(nodes)

      callCount must eventually(be_==(1))
    }

    "when handling a Disconnected message" in {
      "disconnects the cluster" in {
        clusterNotificationManager ! Connected(nodes)
        clusterNotificationManager ! Disconnected

        clusterNotificationManager !? GetCurrentNodes match {
          case CurrentNodes(nodes) => nodes.size must be_==(0)
        }
      }

      "notify listeners" in {
        var callCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Disconnected => callCount += 1
              case _ =>
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Connected(nodes)
        clusterNotificationManager ! Disconnected

        callCount must eventually(be_==(1))
      }

      "do nothing if not connected" in {
        var callCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Disconnected => callCount += 1
              case _ =>
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Disconnected

        callCount must eventually(be_==(0))
      }
    }

    "when handling a Shutdown message stop handling events after shutdown" in {
      var connectedCallCount = 0
      var shutdownCallCount = 0
      val listener = actor {
        loop {
          react {
            case ClusterEvents.Connected(_) => connectedCallCount += 1
            case ClusterEvents.Shutdown => shutdownCallCount += 1
            case _ =>
          }
        }
      }

      clusterNotificationManager ! AddListener(listener)
      clusterNotificationManager ! Connected(nodes)
      clusterNotificationManager ! Shutdown
      clusterNotificationManager ! Connected(nodes)

      connectedCallCount must eventually(be_==(1))
      shutdownCallCount must eventually(be_==(1))
    }
  }
}
