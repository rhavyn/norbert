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
package javacompat
package network

import cluster.{Node, BaseClusterClient}
import com.linkedin.norbert.network.{ResponseIterator, Serializer}
import com.linkedin.norbert.network.client.loadbalancer.{LoadBalancerFactory => SLoadBalancerFactory, LoadBalancer => SLoadBalancer}
import com.linkedin.norbert.network.partitioned.loadbalancer.{PartitionedLoadBalancerFactory => SPartitionedLoadBalancerFactory, PartitionedLoadBalancer => SPartitionedLoadBalancer}
import com.linkedin.norbert.network.common.{Endpoint => SEndpoint}
import EndpointConversions._


abstract class BaseNettyNetworkClient extends BaseNetworkClient {
  val underlying: com.linkedin.norbert.network.common.BaseNetworkClient

  def shutdown = underlying.shutdown

  def broadcastMessage[RequestMsg, ResponseMsg](message: RequestMsg, serializer: Serializer[RequestMsg, ResponseMsg]) = underlying.broadcastMessage(message)(serializer, serializer)

  def sendRequestToNode[RequestMsg, ResponseMsg](request: RequestMsg, node: Node, serializer: Serializer[RequestMsg, ResponseMsg]) =
    underlying.sendRequestToNode(request, node)(serializer, serializer)

//  def registerRequest(requestMessage: Message, responseMessage: Message) = underlying.registerRequest(requestMessage, responseMessage)

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

  val lbf = new SLoadBalancerFactory {
    def newLoadBalancer(endpoints: Set[SEndpoint]) = new SLoadBalancer {
      private val lb = loadBalancerFactory.newLoadBalancer(endpoints)

      def nextNode = Option(lb.nextNode)
    }
  }

  val underlying = if (server == null) {
    com.linkedin.norbert.network.client.NetworkClient(convertConfig(config), lbf)
  } else {
    com.linkedin.norbert.network.client.NetworkClient(convertConfig(config), lbf, server.asInstanceOf[NettyNetworkServer].underlying)
  }

  underlying.start

  def sendRequest[RequestMsg, ResponseMsg](requestMsg: RequestMsg, serializer: Serializer[RequestMsg, ResponseMsg]) =
    underlying.sendRequest(requestMsg)(serializer, serializer)

}

class NettyPartitionedNetworkClient[PartitionedId](config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId],
    server: NetworkServer) extends BaseNettyNetworkClient with PartitionedNetworkClient[PartitionedId] {
  def this(config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]) = this(config, loadBalancerFactory, null)

  val lbf = new SPartitionedLoadBalancerFactory[PartitionedId] {
    def newLoadBalancer(endpoints: Set[SEndpoint]) = new SPartitionedLoadBalancer[PartitionedId] {
      private val lb = loadBalancerFactory.newLoadBalancer(endpoints)

      def nextNode(id: PartitionedId) = Option(lb.nextNode(id))
    }
  }

  val underlying = if (server == null) {
    com.linkedin.norbert.network.partitioned.PartitionedNetworkClient(convertConfig(config), lbf)
  } else {
    com.linkedin.norbert.network.partitioned.PartitionedNetworkClient(convertConfig(config), lbf, server.asInstanceOf[NettyNetworkServer].underlying)
  }

  underlying.start


  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, serializer: Serializer[RequestMsg, ResponseMsg]) =
    underlying.sendRequest(id, request)(serializer, serializer)


  def sendRequest[RequestMsg, ResponseMsg](ids: java.util.Set[PartitionedId], request: RequestMsg, serializer: Serializer[RequestMsg, ResponseMsg]) =
    underlying.sendRequest(ids, request)(serializer, serializer)


  def sendRequest[T, RequestMsg, ResponseMsg](ids: java.util.Set[PartitionedId], request: RequestMsg,
                                              scatterGather: ScatterGatherHandler[RequestMsg, ResponseMsg, T, PartitionedId],
                                              serializer: Serializer[RequestMsg, ResponseMsg]) = {
    underlying.sendRequest(ids, request, (request: RequestMsg, node: com.linkedin.norbert.cluster.Node, ids: Set[PartitionedId]) => {
      val i = new java.util.HashSet[PartitionedId]
      ids.foreach { id => i.add(id) }
      scatterGather.customizeRequest(request, node, i)
    }, (request: RequestMsg, responseIterator: ResponseIterator[ResponseMsg]) => scatterGather.gatherResponses(request, responseIterator))(serializer)
  }


}
