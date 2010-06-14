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
import logging.Logging
import common.{NotificationCenterMessages, ClusterManagerComponent}

trait InMemoryClusterManagerComponent extends ClusterManagerComponent {
  class InMemoryClusterManager extends BaseClusterManager with Actor with Logging {
    import ClusterManagerMessages._

    private var availableNodes = Set.empty[Int]

    def act() = {
      loop {
        react {
          case Connect =>
            log.debug("Connected")
            connected = true
            notificationCenter ! NotificationCenterMessages.SendConnectedEvent(nodes)
            reply(ClusterManagerResponse(None))

          case GetNodes => ifConnected { reply(Nodes(nodes)) }

          case AddNode(node) if (currentNodes.contains(node.id)) => ifConnected {
            reply(ClusterManagerResponse(Some(new InvalidNodeException("A node with id %d already exists".format(node.id)))))
          }

          case AddNode(node) => ifConnected {
            log.debug("Adding node: %s".format(node))
            val n = if (availableNodes.contains(node.id)) node.copy(available = true) else node.copy(available = false)
            currentNodes += (n.id -> n)
            notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
            reply(ClusterManagerResponse(None))
          }

          case RemoveNode(nodeId) => ifConnected {
            log.debug("Removing node with id: %d".format(nodeId))
            currentNodes -= nodeId
            notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
            reply(ClusterManagerResponse(None))
          }

          case MarkNodeAvailable(nodeId) => ifConnected {
            log.debug("Marking node with id %d available".format(nodeId))
            currentNodes.get(nodeId).foreach { node => currentNodes += (nodeId -> node.copy(available = true)) }
            availableNodes += nodeId
            notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
            reply(ClusterManagerResponse(None))
          }

          case MarkNodeUnavailable(nodeId) => ifConnected {
            log.debug("Marking node with id %d unavailable".format(nodeId))
            currentNodes.get(nodeId).foreach { node => currentNodes += (nodeId -> node.copy(available = false)) }
            availableNodes -= nodeId
            notificationCenter ! NotificationCenterMessages.SendNodesChangedEvent(nodes)
            reply(ClusterManagerResponse(None))
          }

          case Shutdown =>
            log.debug("InMemoryClusterManager shut down")
            reply(Shutdown)
            exit

          case m => log.error("Received invalid message: %s".format(m))
        }
      }
    }
  }
}
