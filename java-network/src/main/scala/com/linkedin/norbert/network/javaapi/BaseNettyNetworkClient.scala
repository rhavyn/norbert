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
package com.linkedin.norbert.network.javaapi

import com.google.protobuf.Message
import com.linkedin.norbert.cluster.javaapi.{BaseClusterClient, Node}
import com.linkedin.norbert.cluster.javaapi.Implicits._

abstract class BaseNettyNetworkClient extends BaseNetworkClient {
  val underlying: com.linkedin.norbert.network.common.BaseNetworkClient

  def shutdown = underlying.shutdown

  def broadcastMessage(message: Message) = underlying.broadcastMessage(message)

  def sendMessageToNode(message: Message, node: Node) = underlying.sendMessageToNode(message, node)

  def registerRequest(requestMessage: Message, responseMessage: Message) = underlying.registerRequest(requestMessage, responseMessage)

  protected def convertConfig(config: NetworkClientConfig) = {
    val c = new com.linkedin.norbert.network.client.NetworkClientConfig
    if (config.getClusterClient != null) c.clusterClient = config.getClusterClient.asInstanceOf[BaseClusterClient].underlying
    c.serviceName = config.getServiceName
    c.zooKeeperConnectString = config.getZooKeeperConnectString
    c.zooKeeperSessionTimeoutMillis = config.getZooKeeperSessionTimeoutMillis
    c.connectTimeoutMillis = config.getConnectTimeoutMillis
    c.writeTimeoutMillis = config.getWriteTimeoutMillis
    c.maxConnectionsPerNode = config.getMaxConnectionsPerNode
    c.staleRequestTimeoutMins = config.getStaleRequestTimeoutMins
    c.staleRequestCleanupFrequenceMins = config.getStaleRequestCleanupFrequencyMins

    c
  }
}

class NettyNetworkClient(config: NetworkClientConfig, loadBalancerFactory: LoadBalancerFactory, server: NetworkServer) extends BaseNettyNetworkClient with NetworkClient {
  def this(config: NetworkClientConfig, loadBalancerFactory: LoadBalancerFactory) = this(config, loadBalancerFactory, null)

  val lbf = new com.linkedin.norbert.network.client.loadbalancer.LoadBalancerFactory {
    def newLoadBalancer(nodes: Set[com.linkedin.norbert.cluster.Node]) = new com.linkedin.norbert.network.client.loadbalancer.LoadBalancer {
      private val lb = loadBalancerFactory.newLoadBalancer(nodes)

      def nextNode = Option(lb.nextNode)
    }
  }

  val underlying = if (server == null) {
    com.linkedin.norbert.network.client.NetworkClient(convertConfig(config), lbf)
  } else {
    com.linkedin.norbert.network.client.NetworkClient(convertConfig(config), lbf, server.asInstanceOf[NettyNetworkServer].underlying)
  }

  underlying.start

  def sendMessage(message: Message) = underlying.sendMessage(message)
}

class NettyPartitionedNetworkClient[PartitionedId](config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId],
    server: NetworkServer) extends BaseNettyNetworkClient with PartitionedNetworkClient[PartitionedId] {
  def this(config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]) = this(config, loadBalancerFactory, null)

  val lbf = new com.linkedin.norbert.network.partitioned.loadbalancer.PartitionedLoadBalancerFactory[PartitionedId] {
    def newLoadBalancer(nodes: Set[com.linkedin.norbert.cluster.Node]) = new com.linkedin.norbert.network.partitioned.loadbalancer.PartitionedLoadBalancer[PartitionedId] {
      private val lb = loadBalancerFactory.newLoadBalancer(nodes)

      def nextNode(id: PartitionedId) = Option(lb.nextNode(id))
    }
  }

  val underlying = if (server == null) {
    com.linkedin.norbert.network.partitioned.PartitionedNetworkClient(convertConfig(config), lbf)
  } else {
    com.linkedin.norbert.network.partitioned.PartitionedNetworkClient(convertConfig(config), lbf, server.asInstanceOf[NettyNetworkServer].underlying)
  }

  underlying.start

  def sendMessage[T](ids: java.util.Set[PartitionedId], message: Message, scatterGather: ScatterGatherHandler[T, PartitionedId]) = {
    underlying.sendMessage(ids, message,
      (message, node, ids) => {
        val i = new java.util.HashSet[PartitionedId]
        ids.foreach { id => i.add(id) }
        scatterGather.customizeMessage(message, node, i)
      },
      (message, responseIterator) => scatterGather.gatherResponses(message, responseIterator))
  }

  def sendMessage(ids: java.util.Set[PartitionedId], message: Message) = underlying.sendMessage(ids, message)

  def sendMessage(id: PartitionedId, message: Message) = underlying.sendMessage(id, message)
}
