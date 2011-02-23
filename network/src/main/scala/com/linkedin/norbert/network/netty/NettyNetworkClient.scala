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
package netty

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import java.util.concurrent.Executors
import partitioned.loadbalancer.{PartitionedLoadBalancerFactoryComponent, PartitionedLoadBalancerFactory}
import partitioned.PartitionedNetworkClient
import client.loadbalancer.{LoadBalancerFactoryComponent, LoadBalancerFactory}
import common.{BaseNetworkClient}
import cluster.{ClusterClient, ClusterClientComponent}
import protos.NorbertProtos
import util.NamedPoolThreadFactory
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import client.{ThreadPoolResponseHandler, ResponseHandlerComponent, NetworkClient, NetworkClientConfig}

abstract class BaseNettyNetworkClient(clientConfig: NetworkClientConfig) extends BaseNetworkClient with ClusterClientComponent with NettyClusterIoClientComponent with ResponseHandlerComponent {
  val clusterClient = if (clientConfig.clusterClient != null) clientConfig.clusterClient else ClusterClient(clientConfig.serviceName, clientConfig.zooKeeperConnectString,
    clientConfig.zooKeeperSessionTimeoutMillis)

  private val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("norbert-client-pool-%s".format(clusterClient.serviceName)))
  private val bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(executor, executor))
  private val connectTimeoutMillis = clientConfig.connectTimeoutMillis

  val responseHandler = new ThreadPoolResponseHandler(clientConfig.responseHandlerCorePoolSize,
    clientConfig.responseHandlerMaxPoolSize, clientConfig.responseHandlerKeepAliveTime, clientConfig.responseHandlerMaxWaitingQueueSize)

  private val handler = new ClientChannelHandler(clusterClient.serviceName, clientConfig.maxConnectionsPerNode,
    clientConfig.staleRequestCleanupFrequenceMins, clientConfig.requestStatisticsWindow, clientConfig.outlierMuliplier, clientConfig.outlierConstant,
    responseHandler)

  // TODO why isn't clientConfig visible here?
  bootstrap.setOption("connectTimeoutMillis", connectTimeoutMillis)
  bootstrap.setOption("tcpNoDelay", true)
  bootstrap.setOption("reuseAddress", true)
  bootstrap.setPipelineFactory(new ChannelPipelineFactory {
    private val loggingHandler = new LoggingHandler
    private val protobufDecoder = new ProtobufDecoder(NorbertProtos.NorbertMessage.getDefaultInstance)
    private val frameEncoder = new LengthFieldPrepender(4)
    private val protobufEncoder = new ProtobufEncoder

    def getPipeline = {
      val p = Channels.pipeline

      p.addFirst("logging", loggingHandler)

      p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, 0, 4))
      p.addLast("protobufDecoder", protobufDecoder)

      p.addLast("frameEncoder", frameEncoder)
      p.addLast("protobufEncoder", protobufEncoder)

      p.addLast("requestHandler", handler)

      p
    }
  })

  val clusterIoClient = new NettyClusterIoClient(
    new ChannelPoolFactory(clientConfig.maxConnectionsPerNode, clientConfig.writeTimeoutMillis, bootstrap),
    handler.strategy)

  override def shutdown = {
    if (clientConfig.clusterClient == null) clusterClient.shutdown else super.shutdown
    handler.shutdown
  }
}

class NettyNetworkClient(clientConfig: NetworkClientConfig, val loadBalancerFactory: LoadBalancerFactory) extends BaseNettyNetworkClient(clientConfig) with NetworkClient with LoadBalancerFactoryComponent

class NettyPartitionedNetworkClient[PartitionedId](clientConfig: NetworkClientConfig, val loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]) extends BaseNettyNetworkClient(clientConfig)
    with PartitionedNetworkClient[PartitionedId] with PartitionedLoadBalancerFactoryComponent[PartitionedId]
