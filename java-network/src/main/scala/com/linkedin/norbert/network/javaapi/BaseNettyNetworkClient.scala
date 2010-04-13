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
import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.cluster.javaapi.BaseClusterClient

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
    def newLoadBalancer(nodes: Seq[Node]) = new com.linkedin.norbert.network.client.loadbalancer.LoadBalancer {
      private val lb = loadBalancerFactory.newLoadBalancer(nodes.toArray)

      def nextNode = lb.nextNode match {
        case null => None
        case n => Some(n)
      }
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
    def newLoadBalancer(nodes: Seq[Node]) = new com.linkedin.norbert.network.partitioned.loadbalancer.PartitionedLoadBalancer[PartitionedId] {
      private val lb = loadBalancerFactory.newLoadBalancer(nodes.toArray)

      def nextNode(id: PartitionedId) = lb.nextNode(id) match {
        case null => None
        case n => Some(n)
      }
    }
  }

  val underlying = if (server == null) {
    com.linkedin.norbert.network.partitioned.PartitionedNetworkClient(convertConfig(config), lbf)
  } else {
    com.linkedin.norbert.network.partitioned.PartitionedNetworkClient(convertConfig(config), lbf, server.asInstanceOf[NettyNetworkServer].underlying)
  }

  underlying.start

  def sendMessage[T](ids: java.util.List[PartitionedId], message: Message, scatterGather: ScatterGatherHandler[T, PartitionedId]) = {
    underlying.sendMessage(toSeq(ids), message,
      (message, node, ids) => scatterGather.customizeMessage(message, node, toList(ids)),
      (message, responseIterator) => scatterGather.gatherResponses(message, responseIterator))
  }

  def sendMessage(ids: java.util.List[PartitionedId], message: Message) = underlying.sendMessage(toSeq(ids), message)

  def sendMessage(id: PartitionedId, message: Message) = underlying.sendMessage(id, message)

  private def toSeq(ids: java.util.List[PartitionedId]): Seq[PartitionedId] = {
    val seq = new Array[PartitionedId](ids.size())
    var index = 0
    val iterator = ids.iterator()
    while (iterator.hasNext()) {
      seq(index) = iterator.next()
      index = index + 1
    }
    seq
  }

  private def toList(ids: Seq[PartitionedId]): java.util.List[PartitionedId] = {
    val list = new java.util.ArrayList[PartitionedId]
    for (id <- ids)
      list.add(id)
    list
  }

}
