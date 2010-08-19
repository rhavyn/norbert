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
package router

import common.{ClusterManagerMessages, ClusterManagerClusterClient, ClusterManagerClusterClientSpecification}

class ClusterManagerClusterClientWithRouterSpec extends ClusterManagerClusterClientSpecification {
  var newRouterCalledCount = 0

  var throwInvalidClusterException = false
  var throwException = false

  val router = new Router[Int] {
    def nodesFor(a: Int) = IndexedSeq()
  }

  val clusterClient = new ClusterManagerClusterClient with RouterSupport[Int] {
    protected val routerFactory = new RouterFactory[Int] {
      def newRouter(availableNodes: Set[Node]) = {
        if (throwInvalidClusterException) throw new InvalidClusterException("Invalid")
        else if (throwException) throw new Exception

        newRouterCalledCount += 1
        router
      }
    }

    protected val clusterManager = new TestClusterManager(this)
    ClusterManagerClusterClientWithRouterSpec.this.clusterManager = clusterManager

    def serviceName = "test"
  }

  "An unconnected ClusterManagerClusterClient" should {
    doAfter {
      clusterManager ! ClusterManagerMessages.Shutdown
    }

    "behave like a ClusterManagerClusterClient" in { unconnectedExamples }
  }

  "A connected ClusterManagerClusterClient" should {
    doBefore {
      clusterClient.connect
    }

    doAfter {
      clusterManager ! ClusterManagerMessages.Shutdown
    }

    "behave like a ClusterManagerClusterClient" in { connectedExamples }

    "generate a new router instance if the set of available nodes changes" in {
      clusterClient.nodesDidChange(currentNodes.map(n => n.copy(available = true)))
      newRouterCalledCount must be_==(1)
    }

    "send a NewRouterEvent to listeners" in {
      var newRouterEventCount = 0
      var newRouter: Option[Router[Int]] = null
      clusterClient.addListener(ClusterListener {
        case ClusterEvents.NewRouterEvent(r: Option[Router[Int]]) =>
          newRouter = r
          newRouterEventCount += 1
      })
      val nodes = currentNodes.map(n => n.copy(available = true))

      clusterClient.nodesDidChange(nodes)
      newRouterEventCount must eventually(be_==(1))
      newRouter must beSome.which { r => r must be_==(router) }

      clusterClient.didConnect(nodes.dropRight(1))
      newRouterEventCount must eventually(be_==(2))
      newRouter must beSome.which { r => r must be_==(router) }

      clusterClient.didDisconnect
      newRouterEventCount must eventually(be_==(3))
      newRouter must beNone
    }

    "handle a RouterFactory that throws an InvalidClusterException" in {
      throwInvalidClusterException = true
      var newRouter: Option[Router[Int]] = null
      clusterClient.addListener(ClusterListener {
        case ClusterEvents.NewRouterEvent(r: Option[Router[Int]]) => newRouter = r
      })
      clusterClient.nodesDidChange(currentNodes.map(n => n.copy(available = true)))
      newRouter must eventually(beNone)
    }

    "handle a RouterFactory that throws an Exception" in {
      throwException = true
      var newRouter: Option[Router[Int]] = null
      clusterClient.addListener(ClusterListener {
        case ClusterEvents.NewRouterEvent(r: Option[Router[Int]]) => newRouter = r
      })
      clusterClient.nodesDidChange(currentNodes.map(n => n.copy(available = true)))
      newRouter must eventually(beNone)
    }
  }
}
