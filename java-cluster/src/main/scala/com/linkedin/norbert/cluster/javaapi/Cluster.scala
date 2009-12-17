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
package com.linkedin.norbert.cluster.javaapi

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import com.linkedin.norbert.cluster.{DefaultClusterComponent, InvalidNodeException, Node, ClusterException}
import com.linkedin.norbert.cluster.javaapi.{ClusterListener => JClusterListener, Router => JRouter, RouterFactory => JRouterFactory}
import scala.reflect.BeanProperty
import com.linkedin.norbert.NorbertException

/**
 * A client interface for interacting with a cluster.
 */
trait Cluster {
  /**
   * Retrieves the current list of nodes registered with the cluster.
   *
   * @return the current list of nodes
   */
  @throws(classOf[ClusterException])
  def getNodes: Array[Node]

  /**
   * Retrieves the node associated with the specified address and port
   */
  @throws(classOf[ClusterException])
  def getNodeWithAddress(address: InetSocketAddress): Node

  /**
   * Retrieves the node associated with the specified id
   */
  @throws(classOf[ClusterException])
  def getNodeWithId(nodeId: Int): Node

  /**
   * Returns a router instance that is valid for the current state of the cluster.
   *
   * @return a router instance
   */
  @throws(classOf[ClusterException])
  def getRouter: JRouter

  /**
   * Adds a node to the cluster.
   */
  @throws(classOf[InvalidNodeException])
  def addNode(nodeId: Int, address: InetSocketAddress, partitions: Array[Int]): Node

  /**
   * Removes a node from the cluster.
   */
  @throws(classOf[InvalidNodeException])
  def removeNode(nodeId: Int): Unit

  /**
   * Marks a cluster node as available.
   */
  @throws(classOf[ClusterException])
  def markNodeAvailable(nodeId: Int): Unit

  /**
   * Registers a {@code ClusterListener} with the cluster to receive cluster event callbacks.
   */
  @throws(classOf[ClusterException])
  def addListener(listener: JClusterListener): Unit

  /**
   * Unregisters a {@code ClusterListener} with the cluster
   */
  @throws(classOf[ClusterException])
  def removeListener(listener: JClusterListener): Unit

  /**
   * Shuts down this cluster connection.
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
  @throws(classOf[ClusterException])
  def awaitConnection: Unit

  /**
   * Waits for a connection to the cluster uninterruptibly
   */
  @throws(classOf[ClusterException])
  def awaitConnectionUninterruptibly: Unit

  /**
   * Waits for the connection to the cluster to be established for the specified duration of time.
   *
   * @return true if the connection was established before the timeout, false if the timeout occurred
   */
  @throws(classOf[ClusterException])
  def awaitConnection(timeout: Long, unit: TimeUnit): Boolean
}

class ClusterConfig {
  @BeanProperty var clusterName: String = _
  @BeanProperty var zooKeeperUrls: String = _
  @BeanProperty var zooKeeperSessionTimeout: Int = 30
  @BeanProperty var clusterDisconnectTimeout: Int = 30
  @BeanProperty var routerFactory: JRouterFactory = _

  def validate() {
    if (clusterName == null || zooKeeperUrls == null) throw new NorbertException("clusterName and zooKeeperUrls must be specified")
  }
}

class DefaultCluster(clusterConfig: ClusterConfig) extends JavaClusterHelper {
  clusterConfig.validate
  
  protected object componentRegistry extends {
    val clusterName = clusterConfig.clusterName
    val zooKeeperUrls = clusterConfig.zooKeeperUrls
    val javaRouterFactory = clusterConfig.routerFactory

    override val clusterDisconnectTimeout = clusterConfig.clusterDisconnectTimeout
    override val zooKeeperSessionTimeout = clusterConfig.zooKeeperSessionTimeout
  } with DefaultClusterComponent with JavaRouterHelper
}
