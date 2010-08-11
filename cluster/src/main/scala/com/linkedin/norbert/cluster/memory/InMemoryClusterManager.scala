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

import actors.DaemonActor
import logging.Logging
import common.{ClusterManagerMessages, ClusterManager, ClusterManagerDelegate}

class InMemoryClusterManager(protected val delegate: ClusterManagerDelegate) extends ClusterManager with DaemonActor with Logging {
  def act() = {
    invokeDelegate(delegate.didConnect(Set.empty))

    loop {
      import ClusterManagerMessages._

      react {
        case AddNode(node) if (currentNodes.contains(node.id)) =>
          reply(ClusterManagerResponse(Some(new InvalidNodeException("A node with id %d already exists".format(node.id)))))

        case AddNode(node) =>
          val n = node.copy(available = availableNodeIds.contains(node.id))
          currentNodes += (n.id -> n)
          log.debug("Added node: %s".format(node))
          invokeDelegate(delegate.nodesDidChange(nodeSet))
          reply(ClusterManagerResponse(None))

        case RemoveNode(nodeId) if (!currentNodes.contains(nodeId)) => reply(ClusterManagerResponse(None))

        case RemoveNode(nodeId) =>
          currentNodes -= nodeId
          log.debug("Removed node with id: %d".format(nodeId))
          invokeDelegate(delegate.nodesDidChange(nodeSet))
          reply(ClusterManagerResponse(None))

        case MarkNodeAvailable(nodeId) =>
          val exists = setNodeWithIdAvailabilityTo(nodeId, true)
          availableNodeIds += nodeId
          if (exists) {
            log.debug("Marked node with id %d available".format(nodeId))
            invokeDelegate(delegate.nodesDidChange(nodeSet))
          }
          reply(ClusterManagerResponse(None))

        case MarkNodeUnavailable(nodeId) =>
          val exists = setNodeWithIdAvailabilityTo(nodeId, false)
          availableNodeIds -= nodeId
          if (exists) {
            log.debug("Marked node with id %d unavailable".format(nodeId))
            invokeDelegate(delegate.nodesDidChange(nodeSet))
          }
          reply(ClusterManagerResponse(None))

        case Shutdown =>
          invokeDelegate(delegate.didShutdown)
          log.debug("InMemoryClusterManager shut down")
          exit

        case m => log.error("Received invalid message: %s".format(m))
      }
    }
  }
}
