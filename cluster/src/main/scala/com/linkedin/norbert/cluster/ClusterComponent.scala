/*
 * Copyright 2009 LinkedIn, Inc
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

import java.net.{NetworkInterface, InetSocketAddress}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import com.linkedin.norbert.util.Logging

trait ClusterComponent {
  this: ClusterManagerComponent with ClusterWatcherComponent
          with ZooKeeperMonitorComponent with RouterFactoryComponent =>

  val cluster: Cluster

  sealed trait ClusterEvent
  object ClusterEvents {
    case class Connected(nodes: Seq[Node], router: Option[Router]) extends ClusterEvent
    case class NodesChanged(nodes: Seq[Node], router: Option[Router]) extends ClusterEvent
    case object Disconnected extends ClusterEvent
    case object Shutdown extends ClusterEvent
  }

  trait ClusterListener {
    def handleClusterEvent(event: ClusterEvent): Unit
  }

  /**
   * A client interface for interacting with a cluster.
   */
  trait Cluster {
    /**
     * Retrieves the current list of nodes registered with the cluster.
     *
     * @return the current list of nodes
     */
    def nodes: Seq[Node]

    /**
     * Retrieves the node with the specified node id
     */
    def nodeWithId(nodeId: Int): Option[Node]

    /**
     * Retrieves the node associated with the specified address and port
     */
    def nodeWithAddress(address: InetSocketAddress): Option[Node]

    /**
     * Returns a router instance that is valid for the current state of the cluster.
     *
     * @return a router instance
     */
    def router: Option[Router]

    /**
     * Adds a node to the cluster.
     */
    def addNode(nodeId: Int, address: InetSocketAddress, partitions: Array[Int]): Node

    /**
     * Removes a node from the cluster.
     */
    def removeNode(nodeId: Int): Unit

    /**
     * Marks a cluster node as available.
     */
    def markNodeAvailable(nodeId: Int): Unit

    /**
     * Marks a cluster node with the specified address.  If the address specified is a wildcard address
     * then all the network addresses of the local machine are searched for an address that matches an
     * address of a node.
     */
    def markMyselfAvailable(address: InetSocketAddress): Boolean

    /**
     * Registers a {@code ClusterListener} with the cluster to receive cluster event callbacks.
     */
    def addListener(listener: ClusterListener): Unit

    /**
     * Unregisters a {@code ClusterListener} with the cluster
     */
    def removeListener(listener: ClusterListener): Unit

    /**
     *  Shuts down this cluster connection.
     */
    def shutdown: Unit

    /**
     * Queries whether or not a connection to the cluster is established.
     *
     * @returns true if connected, false otherwise
     */
    def isConnected: Boolean

    /**
     * Queries whether or not this cluster connection has been shut down.
     *
     * @return true if shut down, false otherwise
     */
    def isShutdown: Boolean

    /**
     * Waits for the connection to the cluster to be established.
     */
    def awaitConnection: Unit

    /**
     * Waits for a connection to the cluster uninterruptibly
     */
    def awaitConnectionUninterruptibly: Unit

    /**
     * Waits for the connection to the cluster to be established for the specified duration of time.
     *
     * @return true if the connection was established before the timeout, false if the timeout occurred
     */
    def awaitConnection(timeout: Long, unit: TimeUnit): Boolean
  }

  object Cluster {
    def apply(): Cluster = {
      val cluster = new DefaultCluster
      cluster.start
      cluster.addListener(cluster)
      cluster
    }
  }

  class DefaultCluster extends Cluster with ClusterListener with Logging {

    object CurrentState {
      def empty: CurrentState = CurrentState(Array[Node](), None)
    }
    case class CurrentState(nodes: Seq[Node], router: Option[Router])

    @volatile private var currentState = CurrentState.empty
    @volatile private var connectedLatch = new CountDownLatch(1)
    private val shutdownSwitch = new AtomicBoolean

    def start: Unit = {
      log.info("Starting ClusterManager...")
      clusterManager.start

      log.info("Starting ZooKeeperMonitor...")
      zooKeeperMonitor.start
    }

    def nodes: Seq[Node] = doIfNotShutdown(currentState.nodes)

    def router: Option[Router] = doIfNotShutdown(currentState.router)

    def addNode(nodeId: Int, address: InetSocketAddress, partitions: Array[Int]): Node = doIfConnected(zooKeeperMonitor.addNode(nodeId, address, partitions))

    def removeNode(nodeId: Int) = doIfConnected(zooKeeperMonitor.removeNode(nodeId))

    def markNodeAvailable(nodeId: Int) = doIfConnected(zooKeeperMonitor.markNodeAvailable(nodeId))

    def markMyselfAvailable(address: InetSocketAddress) = doIfConnected {
      val node = nodeWithAddress(address)
      if (node.isDefined) {
        markNodeAvailable(node.get.id)
        true
      } else {
        false
      }
    }

    def awaitConnection: Unit = doIfNotShutdown(connectedLatch.await)

    def awaitConnection(timeout: Long, unit: TimeUnit): Boolean = doIfNotShutdown(connectedLatch.await(timeout, unit))

    def awaitConnectionUninterruptibly: Unit = doIfNotShutdown {
      var completed = false

      while (!completed) {
        try {
          awaitConnection
          completed = true
        } catch {
          case ex: InterruptedException => // do nothing
        }
      }
    }

    def nodeWithId(nodeId: Int): Option[Node] = nodeWith(_.id == nodeId)

    def nodeWithAddress(address: InetSocketAddress): Option[Node] = {
      if (address.getAddress.isAnyLocalAddress) {
        log.ifDebug("Looking for node with wildcard address and port: %d", address.getPort)
        nodeWith { node =>
          implicit def enumeration2Iterator[T](e: java.util.Enumeration[T]): Iterator[T] = new Iterator[T] {
            def next = e.nextElement
            def hasNext = e.hasMoreElements
          }

          val n = for {
            ni <- NetworkInterface.getNetworkInterfaces
            addr <- ni.getInetAddresses
            if (new InetSocketAddress(addr, address.getPort) == node.address)
          } yield node

          n.hasNext
        }
      } else {
        log.ifDebug("Looking for node with address: %s", address)
        nodeWith(_.address == address)
      }
    }

    def shutdown: Unit = {
      if (shutdownSwitch.compareAndSet(false, true)) {
        clusterManager ! ClusterMessages.Shutdown
        clusterWatcher.shutdown
        zooKeeperMonitor.shutdown
      }
    }

    def addListener(listener: ClusterListener): Unit = doIfNotShutdown(clusterManager ! ClusterMessages.AddListener(listener))

    def removeListener(listener: ClusterListener): Unit = doIfNotShutdown(clusterManager ! ClusterMessages.RemoveListener(listener))

    def isConnected: Boolean = !shutdownSwitch.get && connectedLatch.getCount == 0

    def isShutdown: Boolean = shutdownSwitch.get

    def handleClusterEvent(event: ClusterEvent): Unit = {
      import ClusterEvents._

      event match {
        case Connected(nodes, router) =>
          log.ifDebug("Received Connected event with nodes: [%s] and router [%s]", nodes, router)
          currentState = CurrentState(nodes, router)
          connectedLatch.countDown

        case NodesChanged(nodes, router) =>
          log.ifDebug("Received NodesChanged event with nodes: [%s] and router [%s]", nodes, router)
          currentState = CurrentState(nodes, router)

        case Disconnected =>
          log.ifDebug("Recieved Disconnected event")
          connectedLatch = new CountDownLatch(1)
          currentState = CurrentState.empty

        case Shutdown =>
          log.ifDebug("Received Shutdown event")
          currentState = CurrentState.empty
      }
    }

    private def doIfConnected[T](block: => T): T = {
      if (!isConnected) throw new ClusterDisconnectedException("Cluster is disconnected, unable to call") else block
    }

    private def doIfNotShutdown[T](block: => T): T = {
      if (isShutdown) throw new ClusterShutdownException else block
    }

    private def nodeWith(predicate: (Node) => Boolean): Option[Node] = doIfConnected(currentState.nodes.filter(predicate).firstOption)
  }
}
