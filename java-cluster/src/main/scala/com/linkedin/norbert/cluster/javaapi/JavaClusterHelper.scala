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
package com.linkedin.norbert.cluster.javaapi

import com.linkedin.norbert.cluster.{ClusterClientComponent, RouterFactoryComponent, Node}
import com.linkedin.norbert.cluster.javaapi.{Router => JRouter, ClusterListener => JClusterListener, RouterFactory => JRouterFactory}
import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress

/**
 * A mixin trait that provides functionality to help adapt the Java API to the Scala API.
 */
trait JavaRouterHelper extends RouterFactoryComponent {
  val javaRouterFactory: JRouterFactory

  type Id = Int

  class RouterWrapper(val router: JRouter) extends Router {
    def apply(id: Int) = {
      val nodes = router.calculateRoute(id)
      if (nodes == null) None else Some(nodes)
    }
  }

  val routerFactory = if (javaRouterFactory != null) new RouterFactory {
    def newRouter(nodes: Seq[Node]) = new RouterWrapper(javaRouterFactory.newRouter(nodes.toArray))
  } else null
}

/**
 * A mixin trait that provides functionality to help adapt the Java API to the Scala API.
 */
trait JavaClusterHelper extends Cluster {
  protected val componentRegistry: ClusterClientComponent with JavaRouterHelper

  import componentRegistry.{clusterClient, ClusterEvent, ClusterEvents, ClusterListener, Router, RouterWrapper}

  private val jListenerListLock = new AnyRef
  private var jListenerList: List[ClusterListenerWrapper] = Nil

  private implicit def optionRouter2JRouter(router: Option[Router]): JRouter = router match {
    case Some(r: RouterWrapper) => r.router
    case None => null
  }

  def getNodes: Array[Node] = clusterClient.nodes.toArray

  def getNodeWithAddress(address: InetSocketAddress): Node = clusterClient.nodeWithAddress(address) getOrElse(null)

  def getNodeWithId(nodeId: Int) = clusterClient.nodeWithId(nodeId) getOrElse(null)

  def getRouter: JRouter = clusterClient.router

  def addNode(nodeId: Int, address: InetSocketAddress, partitions: Array[Int]): Node = clusterClient.addNode(nodeId, address, partitions)

  def removeNode(nodeId: Int): Unit = clusterClient.removeNode(nodeId)

  def markNodeAvailable(nodeId: Int): Unit = clusterClient.markNodeAvailable(nodeId)

  def addListener(listener: JClusterListener): Unit = {
    val l = new ClusterListenerWrapper(listener)

    clusterClient.addListener(l)

    jListenerListLock.synchronized {
      jListenerList = l :: jListenerList
    }
  }

  def removeListener(listener: JClusterListener): Unit = {
    jListenerListLock.synchronized {
      var removed = jListenerList.find(_.listener eq listener)
      if (removed.isDefined) {
        val l = removed.get
        jListenerList = jListenerList.filter(_ ne l)
        clusterClient.removeListener(l)
      }
    }
  }

  def shutdown: Unit = clusterClient.shutdown

  def isConnected: Boolean = clusterClient.isConnected

  def isShutdown: Boolean = clusterClient.isShutdown

  def awaitConnection: Unit = clusterClient.awaitConnection

  def awaitConnectionUninterruptibly: Unit = clusterClient.awaitConnectionUninterruptibly

  def awaitConnection(timeout: Long, unit: TimeUnit): Boolean = clusterClient.awaitConnection(timeout, unit)

  private class ClusterListenerWrapper(val listener: JClusterListener) extends ClusterListener {
    import ClusterEvents._

    def handleClusterEvent(event: ClusterEvent) = event match {
      case Connected(nodes, router) => listener.handleClusterConnected(nodes.toArray, router)
      case NodesChanged(nodes, router) => listener.handleClusterNodesChanged(nodes.toArray, router)
      case Disconnected => listener.handleClusterDisconnected
      case Shutdown => listener.handleClusterShutdown
    }
  }
}
