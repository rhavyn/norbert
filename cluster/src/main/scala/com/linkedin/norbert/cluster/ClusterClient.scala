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

import actors.Actor._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import jmx.JMX.MBean
import jmx.JMX
import logging.Logging
import zookeeper.ZooKeeperClusterClient

/**
 * ClusterClient companion object provides factory methods for creating a <code>ClusterClient</code> instance.
 */
object ClusterClient {
  def apply(serviceName: String, zooKeeperConnectString: String, zooKeeperSessionTimeoutMillis: Int): ClusterClient = {
    val cc = new ZooKeeperClusterClient(serviceName, zooKeeperConnectString, zooKeeperSessionTimeoutMillis)
    cc.start
    cc
  }
}

/**
 *  The client interface for interacting with a cluster.
 */
trait ClusterClient extends Logging {
  this: ClusterNotificationManagerComponent with ClusterManagerComponent =>

  @volatile private var connectedLatch = new CountDownLatch(1)
  private val shutdownSwitch = new AtomicBoolean
  private val startedSwitch = new AtomicBoolean

  JMX.register(new MBean(classOf[ClusterClientMBean], "serviceName=%s".format(serviceName)) with ClusterClientMBean {
    def getNodes = nodes.map(_.toString).toArray
    def isConnected = ClusterClient.this.isConnected
  })

  /**
   * Starts the cluster.  This method must be called before calling any other methods on the cluster.
   */
  def start: Unit = {
    if (shutdownSwitch.get) throw new ClusterShutdownException

    if (startedSwitch.compareAndSet(false, true)) {
      log.ifInfo("Starting ClusterClient...")

      log.ifDebug("Starting ClusterNotificationManager...")
      clusterNotificationManager.start

      log.ifDebug("Starting ClusterManager...")
      clusterManager.start

      val a = actor {
        loop {
          react {
            case ClusterEvents.Connected(_) => connectedLatch.countDown
            case ClusterEvents.Disconnected => connectedLatch = new CountDownLatch(1)
            case 'quit => exit
            case _ => // do nothing
          }
        }
      }

      clusterNotificationManager !? ClusterNotificationMessages.AddListener(a)

      log.ifInfo("Cluster started")
    }
  }

  /**
   * Retrieves the name of the service running on this cluster
   *
   * @return the name of the service running on this cluster
   */
  def serviceName: String

  /**
   * Retrieves the current list of nodes registered with the cluster.
   *
   * @return the current list of nodes
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def nodes: Set[Node] = doIfConnected {
    clusterNotificationManager !? ClusterNotificationMessages.GetCurrentNodes match {
      case ClusterNotificationMessages.CurrentNodes(nodes) => nodes
    }
  }

  /**
   * Looks up the node with the specified id.
   *
   * @param nodeId the id of the node to find
   *
   * @return <code>Some</code> with the node if found, otherwise <code>None</code>
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def nodeWithId(nodeId: Int): Option[Node] = nodeWith(_.id == nodeId)

  /**
   * Adds a node to the cluster metadata.
   *
   * @param nodeId the id of the node to add
   * @param url the url to be used to send requests to the node
   *
   * @return the newly added node
   * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
   * @throws InvalidNodeException thrown if there is an error adding the new node to the cluster metadata
   */
  def addNode(nodeId: Int, url: String): Node = addNode(nodeId, url, Set[Int]())

  /**
   * Adds a node to the cluster metadata.
   *
   * @param nodeId the id of the node to add
   * @param url the url to be used to send requests to the node
   * @param partitions the partitions for which the node can process requests
   *
   * @return the newly added node
   * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
   * @throws InvalidNodeException thrown if there is an error adding the new node to the cluster metadata
   */
  def addNode(nodeId: Int, url: String, partitions: Set[Int]): Node = doIfConnected {
    if (url == null) throw new NullPointerException

    val node = Node(nodeId, url, false, partitions)
    clusterManager !? ClusterManagerMessages.AddNode(node) match {
      case ClusterManagerMessages.ClusterManagerResponse(Some(ex)) => throw ex
      case ClusterManagerMessages.ClusterManagerResponse(None) => node
    }
  }

  /**
   * Removes a node from the cluster metadata.
   *
   * @param nodeId the id of the node to remove
   *
   * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
   */
  def removeNode(nodeId: Int): Unit = handleClusterManagerResponse {
    clusterManager !? ClusterManagerMessages.RemoveNode(nodeId)
  }

  /**
   * Marks a cluster node as online and available for receiving requests.
   *
   * @param nodeId the id of the node to mark available
   *
   * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
   */
  def markNodeAvailable(nodeId: Int): Unit = handleClusterManagerResponse {
    clusterManager !? ClusterManagerMessages.MarkNodeAvailable(nodeId)
  }

  /**
   * Marks a cluster node as offline and unavailable for receiving requests.
   *
   * @param nodeId the id of the node to mark unavailable
   *
   * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
   */
  def markNodeUnavailable(nodeId: Int): Unit = handleClusterManagerResponse {
    clusterManager !? ClusterManagerMessages.MarkNodeUnavailable(nodeId)
  }

  /**
   * Registers a <code>ClusterListener</code> with the <code>ClusterClient</code> to receive cluster events.
   *
   * @param listener the listener instance to register
   */
  def addListener(listener: ClusterListener): ClusterListenerKey = doIfNotShutdown {
    if (listener == null) throw new NullPointerException

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
   * Unregisters a <code>ClusterListener</code> with the <code>ClusterClient</code>.
   *
   * @param key the key what was returned by <code>addListener</code> when the <code>ClusterListener</code> was
   * registered
   *
   * @return a <code>ClusterListenerKey</code> that can be used to unregister the listener
   */
  def removeListener(key: ClusterListenerKey): Unit = doIfNotShutdown {
    if (key == null) throw new NullPointerException

    clusterNotificationManager ! ClusterNotificationMessages.RemoveListener(key)
  }

  /**
   * Shuts down this <code>ClusterClient</code> instance.  Calling this method causes the <code>ClusterClient</code>
   * to disconnect from ZooKeeper which will, if necessary, cause the node to become unavailable.
   */
  def shutdown: Unit = {
    if (shutdownSwitch.compareAndSet(false, true)) {
      log.ifDebug("Shutting down ZooKeeperManager...")
      clusterManager ! ClusterManagerMessages.Shutdown

      log.ifDebug("Shutting down ClusterNotificationManager...")
      clusterNotificationManager ! ClusterNotificationMessages.Shutdown

      log.ifInfo("Cluster shut down")
    }
  }

  /**
   * Queries whether or not a connection to the cluster is established.
   *
   * @return true if connected, false otherwise
   */
  def isConnected: Boolean = doIfStarted { !isShutdown && connectedLatch.getCount == 0 }

  /**
   * Queries whether or not this <code>ClusterClient</code> has been shut down.
   *
   * @return true if shut down, false otherwise
   * @throws ClusterNotStartedException thrown if the cluster has not been started with the method is called
   */
  def isShutdown: Boolean = doIfStarted { shutdownSwitch.get }

  /**
   * Waits for the connection to the cluster to be established. This method will wait indefinitely for
   * the connection.
   *
   * @throws InterruptedException thrown if the current thread is interrupted while waiting
   */
  def awaitConnection: Unit = doIfNotShutdown(connectedLatch.await)

  /**
   * Waits for the connection to the cluster to be established for the specified duration of time.
   *
   * @param timeout how long to wait before giving up, in terms of <code>unit</code>
   * @param unit the <code>TimeUnit</code> that <code>timeout</code> should be interpreted in
   *
   * @return true if the connection was established before the timeout, false if the timeout occurred
   * @throws InterruptedException thrown if the current thread is interrupted while waiting
   */
  def awaitConnection(timeout: Long, unit: TimeUnit): Boolean = doIfNotShutdown(connectedLatch.await(timeout, unit))

  /**
   * Waits for the connection to the cluster to be established. This method will wait indefinitely for
   * the connection and will swallow any <code>InterruptedException</code>s thrown while waiting.
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

  private def doIfStarted[T](block: => T): T = if (startedSwitch.get) block else throw new ClusterNotStartedException

  private def doIfConnected[T](block: => T): T = doIfNotShutdown { if (isConnected) block else throw new ClusterDisconnectedException }

  private def doIfNotShutdown[T](block: => T): T = doIfStarted { if (isShutdown) throw new ClusterShutdownException else block }

  private def handleClusterManagerResponse(block: => Any): Unit = doIfConnected {
    block match {
      case ClusterManagerMessages.ClusterManagerResponse(Some(ex)) => throw ex
      case ClusterManagerMessages.ClusterManagerResponse(None) => // do nothing
    }
  }

  private def nodeWith(predicate: (Node) => Boolean): Option[Node] = doIfConnected(nodes.filter(predicate).toSeq.headOption)
}

trait ClusterClientMBean {
  def getNodes: Array[String]
  def isConnected: Boolean
}
