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
import org.specs.util.WaitFor
import org.specs.mock.Mockito

class ClusterManagerComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor
        with ClusterManagerComponent with ClusterComponent with ZooKeeperMonitorComponent
        with ClusterWatcherComponent with RouterFactoryComponent {
  val nodes = List(Node(1, "localhost", 31313, Array(1, 2), false),
    Node(2, "localhost", 31314, Array(3, 4), true),
    Node(3, "localhost", 31315, Array(5, 6), false))

  val routerFactory = mock[RouterFactory]
  val clusterManager = mock[ClusterManager]

  val clusterWatcher = null
  val cluster = null
  val zooKeeperMonitor = null
  type Id = Any

  "ClusterManagerComponent" should {
    "when handling a Connected message should" in {
      "generate a new router instance" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        routerFactory.newRouter(List(nodes(1))) returns mock[Router]

        clusterManager ! ClusterMessages.Connected(nodes)
        waitFor(10.ms)

        routerFactory.newRouter(List(nodes(1))) was called
      }

      "correctly handle an InvalidClusterException when creating a new router" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        routerFactory.newRouter(List(nodes(1))) throws new InvalidClusterException("Invalid")

        clusterManager ! ClusterMessages.Connected(nodes)
        waitFor(10.ms)

        routerFactory.newRouter(List(nodes(1))) was called
      }
      
      "notify listeners" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        routerFactory.newRouter(Array(nodes(1))) returns mock[Router]

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(_, _) => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.Connected(nodes)
        waitFor(10.ms)

        listener.callCount must be_==(1)
      }
    }

    "handling an AddListener message" in {
      "not send a Connected event to new listeners if not connected" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(_, _) => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.AddListener(listener)
        waitFor(10.ms)

        listener.callCount must be_==(0)
      }

      "send a Connected event to new listeners if connected" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        routerFactory.newRouter(Array(nodes(1))) returns mock[Router]

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(_, _) => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.Connected(nodes)
        clusterManager ! ClusterMessages.AddListener(listener)
        waitFor(10.ms)

        listener.callCount must be_==(1)
      }
    }

    "handling an RemoveListener message should remove the listener" in {
      val clusterManager = new ClusterManager
      clusterManager.start

      routerFactory.newRouter(Array(nodes(1))) returns mock[Router]

      val listener = new ClusterListener {
        var callCount = 0

        def handleClusterEvent(event: ClusterEvent) = event match {
          case ClusterEvents.Connected(_, _) => callCount += 1
          case ClusterEvents.NodesChanged(_, _) => callCount += 1
          case _ =>
        }
      }

      clusterManager ! ClusterMessages.AddListener(listener)
      clusterManager ! ClusterMessages.Connected(nodes)
      clusterManager ! ClusterMessages.RemoveListener(listener)
      clusterManager ! ClusterMessages.NodesChanged(nodes)
      waitFor(10.ms)

      listener.callCount must be_==(1)
    }

    "handling a NodesChanged message" in {
      "not notify listeners if not connected" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.NodesChanged(_, _) => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.NodesChanged(nodes)
        waitFor(10.ms)

        listener.callCount must be_==(0)
      }

      "correctly handle an InvalidClusterException when creating a new router" in {
        routerFactory.newRouter(List(nodes(1))) throws new InvalidClusterException("Invalid")

        val listener = new ClusterListener {
          var callCount = 0
          var nodes: Seq[Node] = _
          var router: Option[Router] = _

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.NodesChanged(n, r) =>
              callCount += 1
              nodes = n
              router = r
            case _ =>
          }
        }

        val clusterManager = new ClusterManager
        clusterManager.start

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.Connected(nodes)
        clusterManager ! ClusterMessages.NodesChanged(nodes)
        waitFor(10.ms)

        listener.callCount must be_==(1)
        listener.nodes must be_==(nodes)
        listener.router must beNone
      }

      "notify listeners if connected" in {
        val router = mock[Router]
        routerFactory.newRouter(List(nodes(1))) returns router

        val listener = new ClusterListener {
          var callCount = 0
          var nodes: Seq[Node] = _
          var router: Option[Router] = _

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.NodesChanged(n, r) =>
              callCount += 1
              nodes = n
              router = r
            case _ =>
          }
        }

        val clusterManager = new ClusterManager
        clusterManager.start

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.Connected(nodes)
        clusterManager ! ClusterMessages.NodesChanged(nodes)
        waitFor(10.ms)

        listener.callCount must be_==(1)
        listener.nodes must be_==(nodes)
        listener.router must beSome[Router].which(_ must be_==(router))
      }
    }

    "handling a Disconnected message" in {
      "not notify listeners if not connected" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Disconnected => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.Disconnected
        waitFor(10.ms)

        listener.callCount must be_==(0)
      }

      "notifies listeners if connected" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        routerFactory.newRouter(Array(nodes(1))) returns mock[Router]

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(_, _) =>
            case ClusterEvents.Disconnected => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.Connected(nodes)
        clusterManager ! ClusterMessages.Disconnected
        waitFor(20.ms)

        listener.callCount must be_==(1)
      }

      "disconnect the cluster" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        routerFactory.newRouter(Array(nodes(1))) returns mock[Router]

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(_, _) =>
            case ClusterEvents.NodesChanged(_, _) => callCount += 1
            case ClusterEvents.Disconnected => callCount += 1
            case _ =>
          }
        }

        val listener2 = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(_, _) => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.Connected(nodes)
        clusterManager ! ClusterMessages.Disconnected
        clusterManager ! ClusterMessages.NodesChanged(nodes)
        clusterManager ! ClusterMessages.AddListener(listener2)
        clusterManager ! ClusterMessages.Disconnected
        waitFor(10.ms)

        listener.callCount must be_==(1)
        listener2.callCount must be_==(0)
      }
    }

    "handling a Shutdown message" in {
      "notify listeners" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Shutdown => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.Shutdown
        waitFor(10.ms)

        listener.callCount must be_==(1)
      }

      "not respond to messages after shutdown" in {
        val clusterManager = new ClusterManager
        clusterManager.start

        val listener = new ClusterListener {
          var callCount = 0

          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(_, _) => callCount += 1
            case ClusterEvents.Shutdown => callCount += 1
            case _ =>
          }
        }

        clusterManager ! ClusterMessages.AddListener(listener)
        clusterManager ! ClusterMessages.Shutdown
        clusterManager ! ClusterMessages.Connected(nodes)
        waitFor(10.ms)

        listener.callCount must be_==(1)
      }
    }
  }
}
