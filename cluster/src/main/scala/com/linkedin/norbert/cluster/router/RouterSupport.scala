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

import common.ClusterManagerClusterClient

trait RouterSupport[A] extends ClusterManagerClusterClient {
  @volatile private var currentRouter: Option[Router[A]] = None

  protected val routerFactory: RouterFactory[A]

  override protected def newDelegate = new RouterSupportClusterManagerDelegate

  protected class RouterSupportClusterManagerDelegate extends DefaultClusterManagerDelegate {
    import ClusterEvents.NewRouterEvent

    override def didConnect(nodes: Set[Node]) = {
      val available = availableNodes
      super.didConnect(nodes)
      updateRouterIfNecessary(available)
    }

    override def nodesDidChange(nodes: Set[Node]) = {
      val available = availableNodes
      super.nodesDidChange(nodes)
      updateRouterIfNecessary(available)
    }

    override def didShutdown = {
      currentRouter = None
      notificationCenter.postNotification(NewRouterEvent(currentRouter))
      super.didShutdown
    }

    override def didDisconnect = {
      currentRouter = None
      notificationCenter.postNotification(NewRouterEvent(currentRouter))
      super.didDisconnect
    }

    private def updateRouterIfNecessary(available: Set[Node]) {
      if (available != availableNodes) {
        currentRouter = try {
          Some(routerFactory.newRouter(availableNodes))
        } catch {
          case ex: InvalidClusterException =>
            log.info(ex, "Unable to create router because the cluster is in an invalid state")
            None

          case ex: Exception =>
            log.error(ex, "Exception thrown while creating new Router instance")
            None
        }

        notificationCenter.postNotification(NewRouterEvent(currentRouter))
      }
    }
  }
}
