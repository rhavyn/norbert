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
package com.linkedin.norbert.network.client

import com.google.protobuf.Message
import java.util.concurrent.Future
import com.linkedin.norbert.network.NoNodesAvailableException
import com.linkedin.norbert.network.netty.NettyNetworkClient
import loadbalancer.{LoadBalancerFactory, LoadBalancer, LoadBalancerFactoryComponent}
import com.linkedin.norbert.network.common._
import com.linkedin.norbert.cluster._

class NetworkClientConfig {
  var clusterClient: ClusterClient = _
  var serviceName: String = _
  var zooKeeperConnectString: String = _
  var zooKeeperSessionTimeoutMillis = 30000

  var connectTimeoutMillis = 1000
  var writeTimeoutMillis = 100
  var maxConnectionsPerNode = 5
}

object NetworkClient {
  def apply(config: NetworkClientConfig, loadBalancerFactory: LoadBalancerFactory): NetworkClient = {
    val nc = new NettyNetworkClient(config, loadBalancerFactory)
    nc.start
    nc
  }
}

/**
 * The network client interface for interacting with nodes in a cluster.
 */
trait NetworkClient extends BaseNetworkClient {
  this: ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent with LoadBalancerFactoryComponent =>

  @volatile private var loadBalancer: Option[Either[InvalidClusterException, LoadBalancer]] = None

  /**
   * Sends a message to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the message should be sent to.
   *
   * @param message the message to send
   *
   * @returns a future which will become available when a response to the message is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>NetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
   */
  def sendMessage(message: Message): Future[Message] = doIfConnected {
    if (message == null) throw new NullPointerException
    verifyMessageRegistered(message)

    val node = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
      lb => lb.nextNode.getOrElse(throw new NoNodesAvailableException("No node available that can handle the message: %s".format(message))))

    val future = new NorbertFuture
    doSendMessage(node, message, e => future.offerResponse(e))

    future
  }

  protected def updateLoadBalancer(nodes: Seq[Node]) {
    loadBalancer = if (nodes != null && nodes.length > 0) {
      try {
        Some(Right(loadBalancerFactory.newLoadBalancer(nodes)))
      } catch {
        case ex: InvalidClusterException =>
          log.ifInfo(ex, "Unable to create new router instance")
          Some(Left(ex))

        case ex: Exception =>
          val msg = "Exception while creating new router instance"
          log.ifError(ex, msg)
          Some(Left(new InvalidClusterException(msg, ex)))
      }
    } else {
      None
    }
  }
}
