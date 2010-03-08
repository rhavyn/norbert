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

import java.net.{NetworkInterface, InetSocketAddress}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import com.linkedin.norbert.util.Logging
import actors.Actor
import Actor._
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A component which provides the client interface for interacting with a cluster.
 */
trait ClusterComponent extends ClusterListenerComponent with ClusterNotificationManagerComponent with ZooKeeperManagerComponent {
  this: RouterFactoryComponent =>
  
  val cluster: Cluster

  object Cluster {
    def apply(connectString: String, sessionTimeout: Int, clusterName: String): Cluster = {
      val clusterNotificationManager = new ClusterNotificationManager
      val zooKeeperManager = new ZooKeeperManager(connectString, sessionTimeout, clusterName, clusterNotificationManager)
      new Cluster(clusterNotificationManager, zooKeeperManager)
    }
  }
  
  /**
   *  The client interface for interacting with a cluster.
   */
  class Cluster(clusterNotificationManager: Actor, zooKeeperManager: Actor) extends Logging {
    @volatile private var connectedLatch = new CountDownLatch(1)
    private val shutdownSwitch = new AtomicBoolean
    private val started = new AtomicBoolean
    
    /**
     * Starts the cluster.  This method must be called before calling any other methods on the cluster.
     */
    def start: Unit = {
      if (started.compareAndSet(false, true)) {
        log.ifDebug("Starting ClusterNotificationManager...")
        clusterNotificationManager.start

        log.ifDebug("Starting ZooKeeperManager...")
        zooKeeperManager.start

        val a = actor {
          loop {
            react {
              case ClusterEvents.Connected(_, _) => connectedLatch.countDown
              case ClusterEvents.Disconnected => connectedLatch = new CountDownLatch(1)
              case _ => // do nothing
            }
          }
        }

        clusterNotificationManager !? ClusterNotificationMessages.AddListener(a)
      }
    }

    /**
     * Retrieves the current list of nodes registered with the cluster.  The return value may be a
     * <code>Seq</code> with 0 elements if the cluster is not connected.
     *
     * @return the current list of nodes
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def nodes: Seq[Node] = doIfNotShutdown {
      clusterNotificationManager !? ClusterNotificationMessages.GetCurrentNodes match {
        case ClusterNotificationMessages.CurrentNodes(nodes) => nodes
      }
    }

    /**
     * Looks up the node with the specified id.
     *
     * @return <code>Some</code> with the node if found, otherwise <code>None</code>
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def nodeWithId(nodeId: Int): Option[Node] = nodeWith(_.id == nodeId)

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
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def nodeWithAddress(address: InetSocketAddress): Option[Node] = if (address.getAddress.isAnyLocalAddress) {
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
      nodeWith(_.address == address)
    }

    /**
     * Retrieves the current router instance for the cluster.
     *
     * The router returned is valid for the state of the cluster at the point when the method is called.
     *
     * @return <code>Some</code> with the router if the cluster is connected, otherwise <code>None</code>
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def router: Option[Router] = doIfNotShutdown {
      clusterNotificationManager !? ClusterNotificationMessages.GetCurrentRouter match {
        case ClusterNotificationMessages.CurrentRouter(router) => router
      }
    }

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
    def addNode(nodeId: Int, address: InetSocketAddress, partitions: Array[Int]): Node = doIfConnected {
      val node = Node(nodeId, address, partitions, false)
      zooKeeperManager !? ZooKeeperManagerMessages.AddNode(node) match {
        case ZooKeeperManagerMessages.ZooKeeperManagerResponse(Some(ex)) => throw ex
        case ZooKeeperManagerMessages.ZooKeeperManagerResponse(None) => node
      }
    }

    /**
     * Removes a node from the cluster metadata.
     *
     * @param nodeId the id of the node to remove
     *
     * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
     * @throws InvalidNodeException thrown if there is an error removing the new node from the cluster metadata
     */
    def removeNode(nodeId: Int): Unit = handleZooKeeperManagerResponse {
      zooKeeperManager !? ZooKeeperManagerMessages.RemoveNode(nodeId)
    }

    /**
     * Marks a cluster node as online and available for receiving requests.
     *
     * @param nodeId the id of the node to mark available
     *
     * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
     */
    def markNodeAvailable(nodeId: Int): Unit = handleZooKeeperManagerResponse {
      zooKeeperManager !? ZooKeeperManagerMessages.MarkNodeAvailable(nodeId)
    }

    /**
     * Marks a cluster node as offline and unavailable for receiving requests.
     *
     * @param nodeId the id of the node to mark unavailable
     *
     * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
     */
    def markNodeUnavailable(nodeId: Int): Unit = handleZooKeeperManagerResponse {
      zooKeeperManager !? ZooKeeperManagerMessages.MarkNodeUnavailable(nodeId)
    }

    /**
     * Registers a <code>ClusterListener</code> with the <code>Cluster</code> to receive cluster events.
     *
     * @param listener the listener instance to register
     *
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def addListener(listener: ClusterListener): ClusterListenerKey = doIfNotShutdown {
      val a = actor {
        loop {
          receive {
            case event: ClusterEvent => listener.handleClusterEvent(event)
            case 'quit => exit
            case m => log.error("Received invalid message: " + m)
          }
        }
      }

      clusterNotificationManager !? ClusterNotificationMessages.AddListener(a) match {
        case ClusterNotificationMessages.AddedListener(key) => key
      }
    }

    /**
     * Unregisters a <code>ClusterListener</code> with the <code>Cluster</code>.
     *
     * @param key the key what was returned by <code>addListener</code> when the <code>ClusterListener</code> was
     * registered
     *
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def removeListener(key: ClusterListenerKey): Unit = doIfNotShutdown { clusterNotificationManager ! ClusterNotificationMessages.RemoveListener(key) }

    /**
     * Shuts down this <code>Cluster</code> instance.  Calling this method causes the <code>Cluster</code>
     * to disconnect from ZooKeeper which will, if necessary, cause the node to become unavailable.
     */
    def shutdown: Unit = {
      if (shutdownSwitch.compareAndSet(false, true)) {
        log.ifDebug("Shutting down ZooKeeperManager...")
        zooKeeperManager ! ZooKeeperManagerMessages.Shutdown

        log.ifDebug("Shutting down ClusterNotificationManager...")
        clusterNotificationManager ! ClusterNotificationMessages.Shutdown
      }
    }

    /**
     * Queries whether or not a connection to the cluster is established.
     *
     * @return true if connected, false otherwise
     */
    def isConnected: Boolean = doIfStarted { !isShutdown && connectedLatch.getCount == 0 }

    /**
     * Queries whether or not this <code>Cluster</code> has been shut down.
     *
     * @return true if shut down, false otherwise
     */
    def isShutdown: Boolean = doIfStarted { shutdownSwitch.get }

    /**
     * Waits for the connection to the cluster to be established. This method will wait indefinitely for
     * the connection.
     *
     * @throws InterruptedException thrown if the current thread is interrupted while waiting
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def awaitConnection: Unit = doIfNotShutdown(connectedLatch.await)

    /**
     * Waits for the connection to the cluster to be established. This method will wait indefinitely for
     * the connection and will swallow any <code>InterruptedException</code>s thrown while waiting.
     *
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
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

    /**
     * Waits for the connection to the cluster to be established for the specified duration of time.
     *
     * @param timeout how long to wait before giving up, in terms of <code>unit</code>
     * @param unit the <code>TimeUnit</code> that <code>timeout</code> should be interpreted in
     *
     * @return true if the connection was established before the timeout, false if the timeout occurred
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def awaitConnection(timeout: Long, unit: TimeUnit): Boolean = doIfNotShutdown(connectedLatch.await(timeout, unit))

    private def doIfStarted[T](block: => T): T = if (started.get) block else throw new ClusterNotStartedException

    private def doIfConnected[T](block: => T): T = doIfStarted { if (isConnected) block else throw new ClusterDisconnectedException }

    private def doIfNotShutdown[T](block: => T): T = doIfStarted { if (isShutdown) throw new ClusterShutdownException else block }

    private def handleZooKeeperManagerResponse(block: => Any): Unit = doIfConnected {
      block match {
        case ZooKeeperManagerMessages.ZooKeeperManagerResponse(Some(ex)) => throw ex
        case ZooKeeperManagerMessages.ZooKeeperManagerResponse(None) => // do nothing
      }
    }

    private def nodeWith(predicate: (Node) => Boolean): Option[Node] = doIfNotShutdown(nodes.filter(predicate).firstOption)
  }

  /**
   * The default <code>Cluster</code> implementation. Instances should not be created directly, instead
   * call <code>Cluster()</code>.
   */
  class DefaultCluster extends Cluster(null, null)
}
