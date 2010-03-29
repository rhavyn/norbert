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
package com.linkedin.norbert.network.netty

import com.linkedin.norbert.network.common.{MessageRegistry, MessageRegistryComponent}
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import com.linkedin.norbert.protos.NorbertProtos
import java.util.concurrent.Executors
import com.linkedin.norbert.cluster.{ClusterClientComponent, ClusterClient}
import com.linkedin.norbert.util.NamedPoolThreadFactory
import com.linkedin.norbert.network.client.loadbalancer.{LoadBalancerFactory, LoadBalancerFactoryComponent}
import com.linkedin.norbert.network.client.{NetworkClientConfig, NetworkClient}

class NettyNetworkClient(clientConfig: NetworkClientConfig, val loadBalancerFactory: LoadBalancerFactory) extends NetworkClient with ClusterClientComponent with NettyClusterIoClientComponent with MessageRegistryComponent
        with LoadBalancerFactoryComponent{
  val messageRegistry = new MessageRegistry
  val clusterClient = if (clientConfig.clusterClient != null) clientConfig.clusterClient else ClusterClient(clientConfig.serviceName, clientConfig.zooKeeperConnectString,
    clientConfig.zooKeeperSessionTimeoutMillis)

  private val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("norbert-client-pool-%s".format(clusterClient.serviceName)))
  private val bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(executor, executor))
  private val connectTimeoutMillis = clientConfig.connectTimeoutMillis
  // TODO why isn't clientConfig visible here?
  bootstrap.setOption("connectTimeoutMillis", connectTimeoutMillis)
  bootstrap.setOption("tcpNoDelay", true)
  bootstrap.setOption("reuseAddress", true)
  val p = bootstrap.getPipeline
  p.addFirst("logging", new LoggingHandler)

  p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Math.MAX_INT, 0, 4, 0, 4))
  p.addLast("protobufDecoder", new ProtobufDecoder(NorbertProtos.NorbertMessage.getDefaultInstance))

  p.addLast("frameEncoder", new LengthFieldPrepender(4))
  p.addLast("protobufEncoder", new ProtobufEncoder)

  p.addLast("requestHandler", new ClientChannelHandler(messageRegistry))

  val clusterIoClient = new NettyClusterIoClient(new ChannelPoolFactory(clientConfig.maxConnectionsPerNode, clientConfig.writeTimeoutMillis, bootstrap))

  override def shutdown = if (clientConfig.clusterClient == null) clusterClient.shutdown else super.shutdown
}
