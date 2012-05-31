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
import com.linkedin.norbert.cluster.{Node => SNode}
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

    c.requestStatisticsWindow = config.getRequestStatisticsWindow
    c.outlierMuliplier = config.getOutlierMuliplier
    c.outlierConstant = config.getOutlierConstant

    c.responseHandlerCorePoolSize = config.getResponseHandlerCorePoolSize
    c.responseHandlerMaxPoolSize = config.getResponseHandlerMaxPoolSize
    c.responseHandlerKeepAliveTime = config.getResponseHandlerKeepAliveTime
    c.responseHandlerMaxWaitingQueueSize = config.getResponseHandlerMaxWaitingQueueSize

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

      def nodesForOneReplica = {
        val jMap = lb.nodesForOneReplica
        var sMap = Map.empty[com.linkedin.norbert.cluster.Node, Set[Int]]

        val entries = jMap.entrySet.iterator
        while(entries.hasNext) {
          val entry = entries.next
          val node = javaNodeToScalaNode(entry.getKey)
          val set = entry.getValue.foldLeft(Set.empty[Int]) { (s, elem) => s + elem.intValue}

          sMap += (node -> set)
        }
        sMap
      }
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
    underlying.sendRequest(ids: Set[PartitionedId], request)(serializer, serializer)

  def sendRequest[RequestMsg, ResponseMsg](ids: java.util.Set[PartitionedId], requestBuilder: RequestBuilder[PartitionedId, RequestMsg], serializer: Serializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = {
    import collection.JavaConversions._
    underlying.sendRequest(ids: java.util.Set[PartitionedId], (node: SNode, ids: Set[PartitionedId]) => requestBuilder(node, ids))(serializer, serializer)
  }

  def sendRequest[RequestMsg, ResponseMsg, T](ids: java.util.Set[PartitionedId],
                                              requestBuilder: RequestBuilder[PartitionedId, RequestMsg],
                                              scatterGather: ScatterGatherHandler[RequestMsg, ResponseMsg, T, PartitionedId],
                                              serializer: Serializer[RequestMsg, ResponseMsg]) = {
    import collection.JavaConversions._
    underlying.sendRequest(ids,
                           (node: SNode, ids: Set[PartitionedId]) => requestBuilder(node, ids),
                           (responseIterator: ResponseIterator[ResponseMsg]) => scatterGather.gatherResponses(responseIterator))(serializer, serializer)
  }


  def sendRequestToOneReplica[RequestMsg, ResponseMsg](request: RequestMsg, serializer: Serializer[RequestMsg, ResponseMsg]) =
    underlying.sendRequestToOneReplica(request)(serializer, serializer)

  def sendRequestToOneReplica[RequestMsg, ResponseMsg](requestBuilder: RequestBuilder[java.lang.Integer, RequestMsg], serializer: Serializer[RequestMsg, ResponseMsg]) =
    underlying.sendRequestToOneReplica((node: SNode, ids: Set[Int]) => {
      val set = new java.util.HashSet[java.lang.Integer]
      ids.foreach(set.add(_))
      requestBuilder(node, set)
    })(serializer, serializer)
}
