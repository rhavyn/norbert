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

import java.util.concurrent.Executors
import actors.Actor
import com.linkedin.norbert.util.{NamedPoolThreadFactory, Logging}

/**
 * The component which manages the state of the cluster and notifies clients when the state changes.
 */
trait ClusterManagerComponent {
  this: ClusterListenerComponent with RouterFactoryComponent =>

  val clusterManager: Actor

  sealed trait ClusterMessage
  object ClusterMessages {
    case class Connected(nodes: Seq[Node]) extends ClusterMessage
    case class NodesChanged(nodes: Seq[Node]) extends ClusterMessage
    case object Disconnected extends ClusterMessage
    case class AddListener(listener: ClusterListener) extends ClusterMessage
    case class RemoveListener(listener: ClusterListener) extends ClusterMessage
    case object Shutdown extends ClusterMessage
  }

  sealed trait ClusterStatus
  object ClusterStatuses {
    case object CONNECTED extends ClusterStatus
    case object DISCONNECTED extends ClusterStatus
  }

  class ClusterManager extends Actor with Logging {
    import Actor._
    
    private val listenerUpdater = Executors.newSingleThreadExecutor(new NamedPoolThreadFactory("cluster-listener-notification"))

    private var status: ClusterStatus = ClusterStatuses.DISCONNECTED
    private var nodes: Seq[Node] = Array[Node]()
    private var router: Option[Router] = None
    private var listeners: List[ClusterListener] = Nil

    def act() = {
      log.ifDebug("ClusterManager started")

      import ClusterMessages._

      loop {
        receive {
          case Connected(newNodes) =>
            status = ClusterStatuses.CONNECTED
            nodes = newNodes
            updateRouter
            notifyListeners(listeners, ClusterEvents.Connected(nodes, router))

          case NodesChanged(newNodes) => doIfConnected("NodesChanged") {
              nodes = newNodes
              updateRouter
              notifyListeners(listeners, ClusterEvents.NodesChanged(nodes, router))
          }

          case Disconnected => doIfConnected("Disconnected") {
              nodes = Array[Node]()
              status = ClusterStatuses.DISCONNECTED
              router = None
              notifyListeners(listeners, ClusterEvents.Disconnected)
          }

          case AddListener(listener) =>
            if (status == ClusterStatuses.CONNECTED) notifyListeners(List(listener), ClusterEvents.Connected(nodes, router))
            listeners = (listener :: listeners).reverse

          case RemoveListener(listener) => listeners = listeners.filter(_ ne listener)

          case Shutdown =>
            notifyListeners(listeners, ClusterEvents.Shutdown)
            listenerUpdater.shutdown
            log.ifDebug("ClusterManager shut down")
            exit
          
          case msg => log.warn("%s sent unknown message: %s", sender, msg)
        }
      }
    }

    private def doIfConnected(msg: String)(block: => Unit) {
      if (status == ClusterStatuses.CONNECTED) block else log.error("Received message %s by %s while disconnected", msg, sender)
    }

    private def updateRouter {
      router = try {
        if (routerFactory != null) {
          Some(routerFactory.newRouter(nodes.filter(_.available)))
        } else {
          None
        }
      } catch {
        case ex: InvalidClusterException =>
          log.warn(ex, "Exception while creating router")
          None
      }
    }

    private def notifyListeners(toNotify: List[ClusterListener], event: ClusterEvent) {
      listenerUpdater.execute(new Runnable {
        def run = toNotify.foreach(_.handleClusterEvent(event))
      })
    }
  }
}
