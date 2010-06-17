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

import zookeeper.ZooKeeperClusterClient
import logging.Logging
import java.util.concurrent.TimeUnit

/**
 * ClusterClient companion object provides factory methods for creating a <code>ClusterClient</code> instance.
 */
object ClusterClient {
  def apply(serviceName: String, zooKeeperConnectString: String, zooKeeperSessionTimeoutMillis: Int): ClusterClient = {
    val cc = new ZooKeeperClusterClient(serviceName, zooKeeperConnectString, zooKeeperSessionTimeoutMillis)
    cc.connect
    cc
  }
}

/**
 *  The client interface for interacting with a cluster.
 */
trait ClusterClient extends Logging {
  /**
   * Connects to the cluster.  This method must be called before calling any other methods on the cluster.
   *
   * @throws AlreadyConnectedException thrown if connect was already called
   * @throws ClusterException thrown if there is an error while connecting to the cluster
   */
  def connect: Unit

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
   * @throws NotYetConnectedException thrown if this method is called before the connect method is called
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def nodes: Set[Node]

  /**
   * Looks up the node with the specified id.
   *
   * @param nodeId the id of the node to find
   *
   * @return <code>Some</code> with the node if found, otherwise <code>None</code>
   * @throws NotYetConnectedException thrown if this method is called before the connect method is called
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def nodeWithId(nodeId: Int): Option[Node]

  /**
   * Adds a node to the cluster metadata.
   *
   * @param nodeId the id of the node to add
   * @param url the url to be used to send requests to the node
   *
   * @return the newly added node
   * @throws InvalidNodeException thrown if there is an error adding the new node to the cluster metadata
   * @throws NotYetConnectedException thrown if this method is called before the connect method is called
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def addNode(nodeId: Int, url: String, partitions: Set[Int] = Set.empty): Node

  /**
   * Removes a node from the cluster metadata.
   *
   * @param nodeId the id of the node to remove
   *
   * @throws NotYetConnectedException thrown if this method is called before the connect method is called
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def removeNode(nodeId: Int): Unit

  /**
   * Marks a cluster node as online and available for receiving requests.
   *
   * @param nodeId the id of the node to mark available
   *
   * @throws NotYetConnectedException thrown if this method is called before the connect method is called
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def markNodeAvailable(nodeId: Int): Unit

  /**
   * Marks a cluster node as offline and unavailable for receiving requests.
   *
   * @param nodeId the id of the node to mark unavailable
   *
   * @throws NotYetConnectedException thrown if this method is called before the connect method is called
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def markNodeUnavailable(nodeId: Int): Unit

  /**
   * Registers a <code>ClusterListener</code> with the <code>ClusterClient</code> to receive cluster events.
   *
   * @param listener the listener instance to register
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def addListener(listener: ClusterListener): ClusterListenerKey

  /**
   * Unregisters a <code>ClusterListener</code> with the <code>ClusterClient</code>.
   *
   * @param key the key what was returned by <code>addListener</code> when the <code>ClusterListener</code> was
   * registered
   *
   * @return a <code>ClusterListenerKey</code> that can be used to unregister the listener
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def removeListener(key: ClusterListenerKey): Unit

  /**
   * Queries whether or not a connection to the cluster is established.
   *
   * @return true if connected, false otherwise
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def isConnected: Boolean

  /**
   * Queries whether or not this <code>ClusterClient</code> has been shut down.
   *
   * @return true if shut down, false otherwise
   */
  def isShutdown: Boolean

  /**
   * Waits for the connection to the cluster to be established. This method will wait indefinitely for
   * the connection.
   *
   * @throws InterruptedException thrown if the current thread is interrupted while waiting
   * @throws NotYetConnectedException thrown if this method is called before the connect method is called
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   * @throws ClusterShutdownException thrown this method is called after shutdown is called
   */
  def awaitConnection: Unit

  /**
   * Waits for the connection to the cluster to be established for the specified duration of time.
   *
   * @param timeout how long to wait before giving up, in terms of <code>unit</code>
   * @param unit the <code>TimeUnit</code> that <code>timeout</code> should be interpreted in
   *
   * @return true if the connection was established before the timeout, false if the timeout occurred
   * @throws InterruptedException thrown if the current thread is interrupted while waiting
   */
  def awaitConnection(timeout: Long, unit: TimeUnit): Boolean

  /**
   * Waits for the connection to the cluster to be established. This method will wait indefinitely for
   * the connection and will swallow any <code>InterruptedException</code>s thrown while waiting.
   */
  def awaitConnectionUninterruptibly: Unit

  /**
   * Waits for the connection to the cluster to be established. This method will wait indefinitely for
   * the connection and will swallow any <code>InterruptedException</code>s thrown while waiting.
   *
   * @param timeout how long to wait before giving up, in terms of <code>unit</code>
   * @param unit the <code>TimeUnit</code> that <code>timeout</code> should be interpreted in
   *
   * @return true if the connection was established before the timeout, false if the timeout occurred
   */
  def awaitConnectionUninterruptibly(timeout: Long, unit: TimeUnit): Boolean

  /**
   * Shuts down this <code>ClusterClient</code> instance.  Calling this method causes the <code>ClusterClient</code>
   * to disconnect from ZooKeeper which will, if necessary, cause the node to become unavailable.
   */
  def shutdown: Unit
}

trait ClusterClientMBean {
  def getNodes: String
  def isConnected: Boolean
}
