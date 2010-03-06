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
import Actor._

trait ClusterNotificationManagerComponent {
  this: RouterFactoryComponent =>
  
  sealed trait ClusterEvent
  object ClusterEvents {
    /**
     * <code>ClusterEvent</code> which indicates that you are now connected to the cluster.
     *
     * @param nodes the current list of <code>Node</code>s stored in the cluster metadata
     * @param router a <code>Router</code> which is valid for the current state of the cluster
     */
    case class Connected(nodes: Seq[Node], router: Option[Router]) extends ClusterEvent

    /**
     * <code>ClusterEvent</code> which indicates that the cluster topology has changed.
     *
     * @param nodes the current list of <code>Node</code>s stored in the cluster metadata
     * @param router a <code>Router</code> which is valid for the current state of the cluster
     */
    case class NodesChanged(nodes: Seq[Node], router: Option[Router]) extends ClusterEvent

    /**
     * <code>ClusterEvent</code> which indicates that the cluster is now disconnected.
     */
    case object Disconnected extends ClusterEvent

    /**
     * <code>ClusterEvent</code> which indicates that the cluster is now shutdown.
     */
    case object Shutdown extends ClusterEvent
  }
  
  /**
   * A trait to be implemented by classes which wish to receive cluster events.  Register <code>ClusterListener</code>s
   * with <code>Cluster#addListener(listener)</code>.
   */
  trait ClusterListener {
    /**
     * Handle a cluster event.
     *
     * @param event the <code>ClusterEvent</code> to handle
     */
    def handleClusterEvent(event: ClusterEvent): Unit
  }

  case class ClusterListenerKey(id: Long)

  sealed trait ClusterNotificationMessage
  object ClusterNotificationMessages {
    case class AddListener(listener: Actor) extends ClusterNotificationMessage
    case class AddedListener(key: ClusterListenerKey) extends ClusterNotificationMessage
    case class Connected(nodes: Seq[Node]) extends ClusterNotificationMessage
    case object Disconnected extends ClusterNotificationMessage
    case class NodesChanged(nodes: Seq[Node]) extends ClusterNotificationMessage
    case class RemoveListener(key: ClusterListenerKey) extends ClusterNotificationMessage
    case object Shutdown extends ClusterNotificationMessage

    case object GetCurrentRouter extends ClusterNotificationMessage
    case class CurrentRouter(router: Option[Router]) extends ClusterNotificationMessage
    case object GetCurrentNodes extends ClusterNotificationMessage
    case class CurrentNodes(nodes: Seq[Node]) extends ClusterNotificationMessage
  }

  class ClusterNotificationManager extends Actor with Logging {
    private var currentNodes: Seq[Node] = Nil
    private var currentRouter: Option[Router] = None
    private var listeners = Map[ClusterListenerKey, Actor]()
    private var connected = false
    private var listenerId: Long = 0

    def act() = {
      trapExit = true

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
          case GetCurrentRouter => reply(CurrentRouter(currentRouter))
          case GetCurrentNodes => reply(CurrentNodes(currentNodes))
          case m => log.error("Received unknown message: %s", m)
        }
      }
    }

    private def handleAddListener(listener: Actor) {
      log.ifDebug("Handling AddListener(%s) message", listener)

//      val a = actor {
//        loop {
//          self.receive {
//            case event: ClusterEvent => listener.handleClusterEvent(event)
//            case 'quit => exit
//            case m => log.error("Received invalid message: " + m)
//          }
//        }
//      }

      listenerId += 1
      val key = ClusterListenerKey(listenerId)
      listeners += (key -> listener)
      if (connected) listener ! ClusterEvents.Connected(currentNodes, currentRouter)
      reply(ClusterNotificationMessages.AddedListener(key))
    }

    private def handleConnected(nodes: Seq[Node]) {
      log.ifDebug("Handling Connected(%s) message", nodes)
      
      if (connected) {
        log.error("Received a Connected event when already connected")
      } else {
        connected = true
        currentNodes = nodes
        currentRouter = generateRouter

        notifyListeners(ClusterEvents.Connected(currentNodes, currentRouter))
      }
    }

    private def handleDisconnected {
      log.ifDebug("Handling Disconnected message")

      if (connected) {
        connected = false
        currentNodes = Nil
        currentRouter = None

        notifyListeners(ClusterEvents.Disconnected)
      } else {
        log.error("Received a Disconnected event when disconnected")
      }
    }

    private def handleNodesChanged(nodes: Seq[Node]) {
      log.ifDebug("Handling NodesChanged(%s) message", nodes)

      if (connected) {
        currentNodes = nodes
        currentRouter = generateRouter

        notifyListeners(ClusterEvents.NodesChanged(currentNodes, currentRouter))
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
      currentRouter = None
      exit
    }

    private def generateRouter = try {
      routerFactory.newRouter(currentNodes.filter(_.available == true)) match {
        case null => None
        case r => Some(r)
      }
    } catch {
      case ex: InvalidClusterException =>
        log.ifInfo(ex, "Unable to create new router instance")
        None

      case ex: Exception =>
        log.error(ex, "Exception while creating new router instance")
        None
    }

    private def notifyListeners(event: ClusterEvent) = listeners.values.foreach(_ ! event)
  }
}
