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

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.specs.util.WaitFor
import actors.Actor._

class ClusterNotificationManagerComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with ClusterNotificationManagerComponent
        with RouterFactoryComponent with ClusterListenerComponent {
  val routerFactory = mock[RouterFactory]
  type Id = Int
  val clusterNotificationManager = new ClusterNotificationManager

  clusterNotificationManager.start

  val nodes = List(Node(1, "localhost:31313", Array(1, 2), false),
    Node(2, "localhost:31314", Array(3, 4), true),
    Node(3, "localhost:31315", Array(5, 6), false))
  
  "ClusterNotificationManager" should {
    import ClusterNotificationMessages._

    "when handling an AddListener message" in {
      "send a Connected event to the listener if the cluster is connected" in {
        clusterNotificationManager ! Connected(nodes)

        var callCount = 0
        val listener = actor {
          react {
            case ClusterEvents.Connected(_, _) => callCount += 1
          }
        }

        clusterNotificationManager ! AddListener(listener)
        waitFor(20.ms)
        callCount must be_==(1)
      }

      "not send a Connected event to the listener if the cluster is not connected" in {
        var callCount = 0
        val listener = actor {
          react {
            case ClusterEvents.Connected(_, _) => callCount += 1
          }
        }

        clusterNotificationManager ! AddListener(listener)
        waitFor(20.ms)
        callCount must be_==(0)
      }
    }

    "when handling a RemoveListener message remove the listener" in {
      routerFactory.newRouter(Array(nodes(1))) returns mock[Router]

      var callCount = 0
      val listener = actor {
        loop {
          react {
            case ClusterEvents.Connected(_, _) => callCount += 1
            case ClusterEvents.NodesChanged(_, _) => callCount += 1
          }
        }
      }

      val key = clusterNotificationManager !? AddListener(listener) match {
        case AddedListener(key) => key
      }

      clusterNotificationManager ! Connected(nodes)
      clusterNotificationManager ! RemoveListener(key)
      clusterNotificationManager ! NodesChanged(nodes)
      waitFor(20.ms)

      callCount must be_==(1)
    }

    "when handling a Connected message" in {
      "generate a new router instance" in {
        val router = mock[Router]
        routerFactory.newRouter(List(nodes(1))) returns router

        clusterNotificationManager ! Connected(nodes)
        waitFor(20.ms)

        routerFactory.newRouter(List(nodes(1))) was called
        clusterNotificationManager !? GetCurrentNodes match {
          case CurrentNodes(nodes) => nodes must be_==(nodes)
        }
        clusterNotificationManager !? GetCurrentRouter match {
          case CurrentRouter(r) => r must beSome[Router].which(_ must be_==(router))
        }
      }

      "correctly handle an InvalidClusterException when creating a new router" in {
        routerFactory.newRouter(List(nodes(1))) throws new InvalidClusterException("Invalid")

        clusterNotificationManager ! Connected(nodes)
        waitFor(20.ms)

        routerFactory.newRouter(List(nodes(1))) was called
        clusterNotificationManager !? GetCurrentRouter match {
          case CurrentRouter(r) => r must beNone
        }
      }

      "notify listeners" in {
        routerFactory.newRouter(List(nodes(1))) returns mock[Router]

        var callCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Connected(_, _) => callCount += 1
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Connected(nodes)
        waitFor(20.ms)

        callCount must be_==(1)
      }

      "do nothing if already connected" in {
        routerFactory.newRouter(List(nodes(1))) returns mock[Router]

        var callCount = 0
        val listener = actor {
          loop {
            react {
              case ClusterEvents.Connected(_, _) => callCount += 1
              case _ =>
            }
          }
        }

        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! Connected(nodes)
        clusterNotificationManager ! Connected(nodes.dropRight(1))
        waitFor(20.ms)

        callCount must be_==(1)
        routerFactory.newRouter(List(nodes(1))) was called.once
      }
    }

    "when handling a NodesChanged message" in {
      "generate a new router instance" in {
        val router = mock[Router]
        routerFactory.newRouter(List(nodes(1))) returns router

        clusterNotificationManager ! Connected(nodes.dropRight(2))
        clusterNotificationManager ! NodesChanged(nodes)
        waitFor(20.ms)

        routerFactory.newRouter(List(nodes(1))) was called
        clusterNotificationManager !? GetCurrentNodes match {
          case CurrentNodes(nodes) => nodes must be_==(nodes)
        }
        clusterNotificationManager !? GetCurrentRouter match {
          case CurrentRouter(r) => r must beSome[Router].which(_ must be_==(router))
        }
      }

      "correctly handle an InvalidClusterException when creating a new router" in {
        routerFactory.newRouter(List(nodes(1))) throws new InvalidClusterException("Invalid")

        clusterNotificationManager ! Connected(nodes.dropRight(2))
        clusterNotificationManager ! NodesChanged(nodes)
        waitFor(20.ms)

        routerFactory.newRouter(List(nodes(1))) was called
        clusterNotificationManager !? GetCurrentRouter match {
          case CurrentRouter(r) => r must beNone
        }
      }

      "notify listeners" in {
        val router = mock[Router]
        routerFactory.newRouter(List(nodes(1))) returns router

        var callCount = 0
        var currentNodes: Seq[Node] = Nil
        var currentRouter: Option[Router] = None
        val listener = actor {
          loop {
            react {
              case ClusterEvents.NodesChanged(n, r) =>
                callCount += 1
                currentNodes = n
                currentRouter = r
            }
          }
        }

        clusterNotificationManager ! Connected(nodes.dropRight(2))
        clusterNotificationManager ! AddListener(listener)
        clusterNotificationManager ! NodesChanged(nodes)
        waitFor(20.ms)

        callCount must be_==(1)
        currentNodes must be_==(currentNodes)
        currentRouter must beSome[Router].which(_ must be_==(router))
      }
    }

    "do nothing is not connected" in {
      routerFactory.newRouter(List(nodes(1))) throws new InvalidClusterException("Invalid")

      var callCount = 0
      val listener = actor {
        loop {
          react {
            case ClusterEvents.NodesChanged(n, r) => callCount += 1
            case _ =>
          }
        }
      }

      clusterNotificationManager ! Connected(nodes.dropRight(2))
      clusterNotificationManager ! AddListener(listener)
      clusterNotificationManager ! NodesChanged(nodes)
      waitFor(20.ms)

      callCount must be_==(1)
      routerFactory.newRouter(List(nodes(1))) was called.once
    }

    "when handling a Disconnected message" in {
      "disconnects the cluster" in {
        clusterNotificationManager ! Connected(nodes)
        clusterNotificationManager ! Disconnected

        clusterNotificationManager !? GetCurrentNodes match {
          case CurrentNodes(nodes) => nodes.length must be_==(0)
        }
        clusterNotificationManager !? GetCurrentRouter match {
          case CurrentRouter(router) => router must beNone
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
        waitFor(20.ms)

        callCount must be_==(1)
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
        waitFor(20.ms)

        callCount must be_==(0)
      }
    }

    "when handling a Shutdown message stop handling events after shutdown" in {
      var connectedCallCount = 0
      var shutdownCallCount = 0
      val listener = actor {
        loop {
          react {
            case ClusterEvents.Connected(_, _) => connectedCallCount += 1
            case ClusterEvents.Shutdown => shutdownCallCount += 1
            case _ =>
          }
        }
      }

      clusterNotificationManager ! AddListener(listener)
      clusterNotificationManager ! Connected(nodes)
      clusterNotificationManager ! Shutdown
      clusterNotificationManager ! Connected(nodes)
      waitFor(20.ms)

      connectedCallCount must be_==(1)
      shutdownCallCount must be_==(1)
    }
  }
}
