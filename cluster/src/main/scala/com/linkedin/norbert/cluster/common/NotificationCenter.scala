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

import actors.{ReplyReactor, Actor}
import logging.Logging

sealed trait NotificationCenterMessage
object NotificationCenterMessages {
  case class AddListener(listener: ClusterListener) extends NotificationCenterMessage
  case class AddedListener(key: ClusterListenerKey) extends NotificationCenterMessage
  case class RemoveListener(key: ClusterListenerKey) extends NotificationCenterMessage
  case class Connected(nodes: Set[Node]) extends NotificationCenterMessage
  case class NodesChanged(nodes: Set[Node]) extends NotificationCenterMessage
  case object Disconnected extends NotificationCenterMessage
  case object Shutdown extends NotificationCenterMessage

  case object GetCurrentNodes extends NotificationCenterMessage
  case class CurrentNodes(nodes: Set[Node]) extends NotificationCenterMessage
}

private class ClusterListenerActor(listener: ClusterListener) extends ReplyReactor with Logging {
  def act() = {
    loop {
      import ClusterListenerMessages._

      react {
        case HandleEvent(event) =>
          try {
            listener.handleClusterEvent(event)
          } catch {
            case ex: Exception => log.warn(ex, "ClusterListener threw an exception while handling event")
          }

        case Shutdown =>
          reply(Shutdown)
          exit
      }
    }
  }
}

private sealed trait ClusterListenerMessage
private object ClusterListenerMessages {
  case class HandleEvent(event: ClusterEvent) extends ClusterListenerMessage
  case object Shutdown
}

class NotificationCenter extends Actor with Logging {
  private var currentNodes = Set.empty[Node]
  private var listeners = Map.empty[ClusterListenerKey, ClusterListenerActor]
  private var connected = false
  private var listenerId = 0L

  def act() = {
    log.debug("NotificationCenter started")

    loop {
      import NotificationCenterMessages._

      react {
        case AddListener(listener) => handleAddListener(listener)
        case RemoveListener(key) => handleRemoveListener(key)
        case Connected(nodes) => handleConnected(nodes)
        case NodesChanged(nodes) => handleNodesChanged(nodes)
        case Disconnected => handleDisconnected
        case Shutdown => handleShutdown
        case msg => log.error("Received invalid message %s".format(msg))
      }
    }
  }

  private def handleAddListener(listener: ClusterListener) {
    import ClusterListenerMessages._

    listenerId += 1
    val key = ClusterListenerKey(listenerId)

    log.debug("Adding listener %s with key %s".format(listener, key))

    val a = new ClusterListenerActor(listener)
    a.start
    listeners += (key -> a)

    if (connected) {
      log.debug("Sending Connected event to new listener")
      a ! HandleEvent(ClusterEvents.Connected(availableNodes))
    }

    reply(NotificationCenterMessages.AddedListener(key))
  }

  private def handleRemoveListener(key: ClusterListenerKey) {
    log.debug("Removing listener with key: %s".format(key))

    listeners.get(key).foreach { l =>
      log.debug("Shutting down listener: %s".format(l))
      l ! ClusterListenerMessages.Shutdown
      listeners -= key
    }
  }

  private def handleConnected(nodes: Set[Node]) {
    if (connected) {
      log.error("Received a connected event when already connected with nodes: %s".format(nodes))
    } else {
      log.debug("Received connected event with nodes: %s".format(nodes))

      currentNodes = nodes
      connected = true

      log.debug("Sending Connected event to listeners")
      notifyListeners(ClusterEvents.Connected(availableNodes))
    }
  }

  private def handleNodesChanged(nodes: Set[Node]) {
    if (connected) {
      log.debug("Received a nodes changed event with nodes: %s".format(nodes))
      currentNodes = nodes
      notifyListeners(ClusterEvents.NodesChanged(availableNodes))
    } else {
      log.error("Received nodes changed event when disconnected with nodes: %s".format(nodes))
    }
  }

  private def handleDisconnected {
    if (connected) {
      currentNodes = Set.empty
      connected = false
      notifyListeners(ClusterEvents.Disconnected)
    } else {
      log.error("Received a disconnected event when already disconnected")
    }
  }

  private def handleShutdown {
    import ClusterListenerMessages._

    log.debug("Shutting down NotificationCenter...")

    log.debug("Sending Shutdown event to listeners")
    notifyListeners(ClusterEvents.Shutdown)

    log.debug("Shutting down listeners")
    listeners.foreach { case (_, a) => a !? Shutdown }

    log.debug("NotificationCenter shut down")
    exit
  }

  private def availableNodes = currentNodes.filter(_.available)

  private def notifyListeners(event: ClusterEvent) = listeners.foreach { case (_, a) => a ! ClusterListenerMessages.HandleEvent(event) }
}
