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

/**
 * A component which provides the client interface for interacting with a cluster.
 */
trait ClusterComponent {
  this: ClusterManagerComponent with ClusterWatcherComponent
          with ZooKeeperMonitorComponent with RouterFactoryComponent =>

  val cluster: Cluster

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

  /**
   *  The client interface for interacting with a cluster.
   */
  trait Cluster {
    /**
     * Retrieves the current list of nodes registered with the cluster.  The return value may be a
     * <code>Seq</code> with 0 elements if the cluster is not connected.
     *
     * @return the current list of nodes
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def nodes: Seq[Node]

    /**
     * Looks up the node with the specified id.
     *
     * @return <code>Some</code> with the node if found, otherwise <code>None</code>
     * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
     */
    def nodeWithId(nodeId: Int): Option[Node]

    /**
     * Looks up the node with the specified <code>InetSocketAddress</code>.
     *
     * If the <code>InetSocketAddress</code> specified is a wildcard address, a node is considered a match
     * if it has the same port as specified in the <code>InetSocketAddress</code> and the IP address of the
     * node matches any of the IP addresses assigned to the local machine.  If the <code>InetSocketAddress</code>
     * is not a wildcard address, a node is considered a match if it has exactly the same address and port
     * as specified.
     *
     * @return <code>Some</code> with the node if found, otherwise <code>None</code>
     * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
     */
    def nodeWithAddress(address: InetSocketAddress): Option[Node]

    /**
     * Retrieves the current router instance for the cluster.
     *
     * The router returned is valid for the state of the cluster at the point when the method is called.
     *
     * @return <code>Some</code> with the router if the cluster is connected, otherwise <code>None</code>
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def router: Option[Router]

    /**
     * Adds a node to the cluster metadata.
     *
     * @param nodeId the id of the node to add
     * @param address the address to be used to send requests to the node
     * @param partitions the partitions for which the node can process requests
     *
     * @return the newly added node
     * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
     * @throws InvalidNodeException thrown if there is an error adding the new node to the cluster metadata
     */
    def addNode(nodeId: Int, address: InetSocketAddress, partitions: Array[Int]): Node

    /**
     * Removes a node from the cluster metadata.
     *
     * @param nodeId the id of the node to remove
     *
     * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
     * @throws InvalidNodeException thrown if there is an error removing the new node from the cluster metadata
     */
    def removeNode(nodeId: Int): Unit

    /**
     * Marks a cluster node as online and available for receiving requests.
     *
     * @param nodeId the id of the node to mark available
     *
     * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
     */
    def markNodeAvailable(nodeId: Int): Unit

    /**
     * Registers a <code>ClusterListener</code> with the <code>Cluster</code> to receive cluster events.
     *
     * @param listener the listener instance to register
     *
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def addListener(listener: ClusterListener): Unit

    /**
     * Unregisters a <code>ClusterListener</code> with the <code>Cluster</code>.
     *
     * @param listener the listener instance to unregister.  This must be the same instance as was
     * passed to <code>addListener</code>.
     *
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def removeListener(listener: ClusterListener): Unit

    /**
     * Shuts down this <code>Cluster</code> instance.  Calling this method causes the <code>Cluster</code>
     * to disconnect from ZooKeeper which will, if necessary, cause the node to become unavailable.
     */
    def shutdown: Unit

    /**
     * Queries whether or not a connection to the cluster is established.
     *
     * @return true if connected, false otherwise
     */
    def isConnected: Boolean

    /**
     * Queries whether or not this <code>Cluster</code> has been shut down.
     *
     * @return true if shut down, false otherwise
     */
    def isShutdown: Boolean

    /**
     * Waits for the connection to the cluster to be established. This method will wait indefinitely for
     * the connection.
     *
     * @throws InterruptedException thrown if the current thread is interrupted while waiting
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def awaitConnection: Unit

    /**
     * Waits for the connection to the cluster to be established. This method will wait indefinitely for
     * the connection and will swallow any <code>InterruptedException</code>s thrown while waiting.
     *
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def awaitConnectionUninterruptibly: Unit

    /**
     * Waits for the connection to the cluster to be established for the specified duration of time.
     *
     * @param timeout how long to wait before giving up, in terms of <code>unit</code>
     * @param unit the <code>TimeUnit</code> that <code>timeout</code> should be interpreted in
     *
     * @return true if the connection was established before the timeout, false if the timeout occurred
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def awaitConnection(timeout: Long, unit: TimeUnit): Boolean
  }

  /**
   * Factory for creating <code>Cluster</code> instances. Users are discouraged from using directly, instead
   * mix in the <code>DefaultClusterComponent</code> when creating your component registry.
   */
  object Cluster {
    def apply(): Cluster = {
      val cluster = new DefaultCluster
      cluster.start
      cluster.addListener(cluster)
      cluster
    }
  }

  /**
   * The default <code>Cluster</code> implementation. Instances should not be created directly, instead
   * call <code>Cluster()</code>.
   */
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
