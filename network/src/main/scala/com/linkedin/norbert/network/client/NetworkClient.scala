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
package network
package client

import java.util.concurrent.Future
import loadbalancer.{LoadBalancerFactory, LoadBalancer, LoadBalancerFactoryComponent}
import server.{MessageExecutorComponent, NetworkServer}
import cluster._
import network.common._
import netty.NettyNetworkClient

class NetworkClientConfig {
  var clusterClient: ClusterClient = _
  var serviceName: String = _
  var zooKeeperConnectString: String = _
  var zooKeeperSessionTimeoutMillis = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT_MILLIS

  var connectTimeoutMillis = NetworkDefaults.CONNECT_TIMEOUT_MILLIS
  var writeTimeoutMillis = NetworkDefaults.WRITE_TIMEOUT_MILLIS
  var maxConnectionsPerNode = NetworkDefaults.MAX_CONNECTIONS_PER_NODE
  var staleRequestTimeoutMins = NetworkDefaults.STALE_REQUEST_TIMEOUT_MINS
  var staleRequestCleanupFrequenceMins = NetworkDefaults.STALE_REQUEST_CLEANUP_FREQUENCY_MINS
  val outlierMuliplier = NetworkDefaults.OUTLIER_MULTIPLIER
  val outlierConstant = NetworkDefaults.OUTLIER_CONSTANT

}
object NetworkClientConfig {
  var MAXRESPONETIME: Int = ClusterDefaults.MAXREPSONETIME
  def calculateScore(t: Int) = {
      val score: Double = (10 - (t * 9.0 / NetworkClientConfig.MAXRESPONETIME ) )
      val healthScore = if (score < 0) 0 else score.intValue
      healthScore

  }
}

object NetworkClient {
  def apply(config: NetworkClientConfig, loadBalancerFactory: LoadBalancerFactory): NetworkClient = {
    val nc = new NettyNetworkClient(config, loadBalancerFactory)
    nc.start
    nc
  }

  def apply(config: NetworkClientConfig, loadBalancerFactory: LoadBalancerFactory, server: NetworkServer): NetworkClient = {
    val nc = new NettyNetworkClient(config, loadBalancerFactory) with LocalMessageExecution with MessageExecutorComponent {
      val messageExecutor = server.asInstanceOf[MessageExecutorComponent].messageExecutor
      val myNode = server.myNode
    }
    nc.start
    nc
  }
}

/**
 * The network client interface for interacting with nodes in a cluster.
 */
trait NetworkClient extends BaseNetworkClient {
  this: ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent =>

  @volatile private var loadBalancer: Option[Either[InvalidClusterException, LoadBalancer]] = None

  /**
   * Sends a request to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the request should be sent to.
   *
   * @param request the message to send
   * @param callback a method to be called with either a Throwable in the case of an error along
   * the way or a ResponseMsg representing the result
   *
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit)
  (implicit serializer: Serializer[RequestMsg, ResponseMsg]): Unit = doIfConnected {
    if (request == null) throw new NullPointerException

    val node = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
      lb => lb.nextNode.getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(request))))

    doSendRequest(node, request, callback)
  }

  /**
   * Sends a request to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the request should be sent to.
   *
   * @param request the message to send
   *
   * @return a future which will become available when a response to the request is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg)
  (implicit serializer: Serializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapter[ResponseMsg]
    sendRequest(request, future)
    future
  }

  protected def updateLoadBalancer(nodes: Set[Endpoint]) {
    loadBalancer = if (nodes != null && nodes.size > 0) {
      try {
        Some(Right(loadBalancerFactory.newLoadBalancer(nodes)))
      } catch {
        case ex: InvalidClusterException =>
          log.info(ex, "Unable to create new router instance")
          Some(Left(ex))

        case ex: Exception =>
          val msg = "Exception while creating new router instance"
          log.error(ex, msg)
          Some(Left(new InvalidClusterException(msg, ex)))
      }
    } else {
      None
    }
  }
}
