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

import com.linkedin.norbert.util.Logging
import actors.Actor

trait ClusterNotificationManagerComponent {
  val clusterNotificationManager: Actor

  sealed trait ClusterNotificationMessage

  object ClusterNotificationMessages {
    case class AddListener(listener: Actor) extends ClusterNotificationMessage
    case class AddedListener(key: ClusterListenerKey) extends ClusterNotificationMessage
    case class Connected(nodes: Seq[Node]) extends ClusterNotificationMessage
    case object Disconnected extends ClusterNotificationMessage
    case class NodesChanged(nodes: Seq[Node]) extends ClusterNotificationMessage
    case class RemoveListener(key: ClusterListenerKey) extends ClusterNotificationMessage
    case object Shutdown extends ClusterNotificationMessage

    case object GetCurrentNodes extends ClusterNotificationMessage
    case class CurrentNodes(nodes: Seq[Node]) extends ClusterNotificationMessage
  }

  class ClusterNotificationManager extends Actor with Logging {
    private var currentNodes: Seq[Node] = Nil
    private var listeners = Map[ClusterListenerKey, Actor]()
    private var connected = false
    private var listenerId: Long = 0

    def act() = {
      log.ifDebug("ClusterNotificationManager started")

      while(true) {
        import ClusterNotificationMessages._

        receive {
          case AddListener(listener) => handleAddListener(listener)
          case Connected(nodes) => handleConnected(nodes)
          case Disconnected => handleDisconnected
          case NodesChanged(nodes) => handleNodesChanged(nodes)
          case RemoveListener(key) => handleRemoveListener(key)
          case Shutdown => handleShutdown
          case GetCurrentNodes => reply(CurrentNodes(currentNodes))
          case m => log.error("Received unknown message: %s", m)
        }
      }
    }

    private def handleAddListener(listener: Actor) {
      log.ifDebug("Handling AddListener(%s) message", listener)

      listenerId += 1
      val key = ClusterListenerKey(listenerId)
      listeners += (key -> listener)
      if (connected) listener ! ClusterEvents.Connected(availableNodes)
      reply(ClusterNotificationMessages.AddedListener(key))
    }

    private def handleConnected(nodes: Seq[Node]) {
      log.ifDebug("Handling Connected(%s) message", nodes)

      if (connected) {
        log.error("Received a Connected event when already connected")
      } else {
        connected = true
        currentNodes = nodes

        notifyListeners(ClusterEvents.Connected(availableNodes))
      }
    }

    private def handleDisconnected {
      log.ifDebug("Handling Disconnected message")

      if (connected) {
        connected = false
        currentNodes = Nil

        notifyListeners(ClusterEvents.Disconnected)
      } else {
        log.error("Received a Disconnected event when disconnected")
      }
    }

    private def handleNodesChanged(nodes: Seq[Node]) {
      log.ifDebug("Handling NodesChanged(%s) message", nodes)

      if (connected) {
        currentNodes = nodes

        notifyListeners(ClusterEvents.NodesChanged(availableNodes))
      } else {
        log.error("Received a NodesChanged event when disconnected")
      }
    }

    private def handleRemoveListener(key: ClusterListenerKey) {
      log.ifDebug("Handling RemoveListener(%s) message", key)
      listeners.get(key) match {
        case Some(a) =>
          a ! 'quit
          listeners -= key

        case None => log.ifInfo("Attempt to remove an unknown listener with key: %s", key)
      }
    }

    private def handleShutdown {
      log.ifDebug("Handling Shutdown message")

      notifyListeners(ClusterEvents.Shutdown)
      listeners.values.foreach(_ ! 'quit)
      currentNodes = Nil
      exit
    }

    private def notifyListeners(event: ClusterEvent) = listeners.values.foreach(_ ! event)

    private def availableNodes = currentNodes.filter(_.available == true)
  }
}
