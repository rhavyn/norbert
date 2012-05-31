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
package memory

import actors.Actor
import Actor._
import common.{ClusterManagerComponent, ClusterNotificationManagerComponent, ClusterManagerHelper}

trait InMemoryClusterManagerComponent extends ClusterManagerComponent with ClusterManagerHelper {
  this: ClusterNotificationManagerComponent =>

  class InMemoryClusterManager extends Actor {
    private var currentNodes = scala.collection.mutable.Map[Int, Node]()
    private var available = scala.collection.mutable.Set[Int]()

    def act() = {
      actor {
        // Give the ClusterNotificationManager a chance to start
        Thread.sleep(100)
        clusterNotificationManager ! ClusterNotificationMessages.Connected(currentNodes)
      }

      while (true) {
        import ClusterManagerMessages._

        receive {
          case AddNode(node) => if (currentNodes.contains(node.id)) {
            reply(ClusterManagerResponse(Some(new InvalidNodeException("A node with id %d already exists".format(node.id)))))
          } else {
            val n = if (available.contains(node.id)) node.copy(available = true) else node.copy(available = false)

            currentNodes += (n.id -> n)
            clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
            reply(ClusterManagerResponse(None))
          }

          case RemoveNode(nodeId) =>
            currentNodes -= nodeId
            clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
            reply(ClusterManagerResponse(None))

          case MarkNodeAvailable(nodeId) =>
            currentNodes.get(nodeId).foreach { node => currentNodes.update(nodeId, node.copy(available = true)) }
            available += nodeId
            clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
            reply(ClusterManagerResponse(None))

          case MarkNodeUnavailable(nodeId) =>
            currentNodes.get(nodeId).foreach { node => currentNodes.update(nodeId, node.copy(available = false)) }
            available -= nodeId
            clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
            reply(ClusterManagerResponse(None))

          case Shutdown => exit
        }
      }
    }
  }
}
