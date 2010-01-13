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

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import com.linkedin.norbert.cluster.javaapi.{ClusterListener => JClusterListener, Router => JRouter, RouterFactory => JRouterFactory}
import scala.reflect.BeanProperty
import com.linkedin.norbert.NorbertException
import com.linkedin.norbert.cluster._

/**
 * The client interface for interacting with a cluster.
 */
trait Cluster {
  /**
   * Retrieves the current list of nodes registered with the cluster.  The return value may be a
   * <code>Seq</code> with 0 elements if the cluster is not connected.
   *
   * @return the current list of nodes
   * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
   */
  @throws(classOf[ClusterShutdownException])
  def getNodes: Array[Node]

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
  @throws(classOf[ClusterDisconnectedException])
  def getNodeWithAddress(address: InetSocketAddress): Node

  /**
   * Looks up the node with the specified id.
   *
   * @return <code>Some</code> with the node if found, otherwise <code>None</code>
   * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
   */
  @throws(classOf[ClusterDisconnectedException])
  def getNodeWithId(nodeId: Int): Node

  /**
   * Retrieves the current router instance for the cluster.
   *
   * The router returned is valid for the state of the cluster at the point when the method is called.
   *
   * @return <code>Some</code> with the router if the cluster is connected, otherwise <code>None</code>
   * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
   */
  @throws(classOf[ClusterShutdownException])
  def getRouter: JRouter

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
  @throws(classOf[ClusterDisconnectedException])
  @throws(classOf[InvalidNodeException])
  def addNode(nodeId: Int, address: InetSocketAddress, partitions: Array[Int]): Node

  /**
   * Removes a node from the cluster metadata.
   *
   * @param nodeId the id of the node to remove
   *
   * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
   * @throws InvalidNodeException thrown if there is an error removing the new node from the cluster metadata
   */
  @throws(classOf[ClusterDisconnectedException])
  @throws(classOf[InvalidNodeException])
  def removeNode(nodeId: Int): Unit

  /**
   * Marks a cluster node as online and available for receiving requests.
   *
   * @param nodeId the id of the node to mark available
   *
   * @throws ClusterDisconnectedException thrown if the cluster is disconnected when the method is called
   */
  @throws(classOf[ClusterDisconnectedException])
  def markNodeAvailable(nodeId: Int): Unit

  /**
   * Registers a <code>ClusterListener</code> with the <code>Cluster</code> to receive cluster events.
   *
   * @param listener the listener instance to register
   *
   * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
   */
  @throws(classOf[ClusterShutdownException])
  def addListener(listener: JClusterListener): Unit

  /**
   * Unregisters a <code>ClusterListener</code> with the <code>Cluster</code>.
   *
   * @param listener the listener instance to unregister.  This must be the same instance as was
   * passed to <code>addListener</code>.
   *
   * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
   */
  @throws(classOf[ClusterShutdownException])
  def removeListener(listener: JClusterListener): Unit

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
  @throws(classOf[ClusterShutdownException])
  @throws(classOf[InterruptedException])
  def awaitConnection: Unit

  /**
   * Waits for the connection to the cluster to be established. This method will wait indefinitely for
   * the connection and will swallow any <code>InterruptedException</code>s thrown while waiting.
   *
   * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
   */
  @throws(classOf[ClusterShutdownException])
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
  @throws(classOf[ClusterShutdownException])
  def awaitConnection(timeout: Long, unit: TimeUnit): Boolean
}

/**
 * JavaBean which provides configuration properties exposed by <code>DefaultCluster</code>.
 */
class ClusterConfig {
  /**
   * The name of the cluster. This property must be set.
   */
  @BeanProperty var clusterName: String = _

  /**
   * The URL to use to connect to ZooKeeper. This property must be set.
   */
  @BeanProperty var zooKeeperUrls: String = _

  /**
   * The ZooKeeper session timeout in milliseconds. Defaults to 30000.
   */
  @BeanProperty var zooKeeperSessionTimeout: Int = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT

  /**
   * The cluster disconnect timeout in seconds.  Defaults to 30000.
   */
  @BeanProperty var clusterDisconnectTimeout: Int = ClusterDefaults.CLUSTER_DISCONNECT_TIMEOUT

  /**
   * The <code>RouterFactory</code> the <code>Cluster</code> should use to generate <code>Router</code> instances.
   * This property defaults to null.
   */
  @BeanProperty var routerFactory: JRouterFactory = _

  /**
   * Validates that the config properties are set correctly.
   *
   * @throws NorbertException thrown if the config properties are not correctly set
   */
  @throws(classOf[NorbertException])
  def validate() {
    if (clusterName == null || zooKeeperUrls == null) throw new NorbertException("clusterName and zooKeeperUrls must be specified")
  }
}

/**
 * A bootstrap for creating a <code>Cluster</code> instance.
 *
 * @param clusterConfig the <code>ClusterConfig</code> to use to configure the new instance
 *
 * @throws NorbertException thrown if the <code>ClusterConfig</code> provided is not valid
 */
@throws(classOf[NorbertException])
class ClusterBootstrap(clusterConfig: ClusterConfig) {
  clusterConfig.validate
  
  protected object componentRegistry extends {
    val clusterName = clusterConfig.clusterName
    val zooKeeperUrls = clusterConfig.zooKeeperUrls
    val javaRouterFactory = clusterConfig.routerFactory
    val clusterDisconnectTimeout = clusterConfig.clusterDisconnectTimeout
    val zooKeeperSessionTimeout = clusterConfig.zooKeeperSessionTimeout
  } with DefaultClusterComponent with JavaRouterHelper

  private val cluster = new JavaClusterHelper {
    lazy val componentRegistry = ClusterBootstrap.this.componentRegistry
  }

  /**
   * Retrieves the <code>Cluster</code> instance generated by the bootstrap.
   *
   * @return a <code>Cluster</code> instance.  The instance returned my not yet have finished
   * connecting to the cluster. Calling one of the await methods is highly recommended before
   * attempting to interact with the instance.
   */
  def getCluster: Cluster = {
    componentRegistry.cluster.start
    cluster
  }
}
