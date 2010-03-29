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

import com.linkedin.norbert.network.client.NetworkClient
import com.linkedin.norbert.network.common.{MessageRegistry, MessageRegistryComponent}
import com.linkedin.norbert.network.client.loadbalancer.LoadBalancerFactoryComponent
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import com.linkedin.norbert.protos.NorbertProtos
import java.util.concurrent.Executors
import com.linkedin.norbert.cluster.{ClusterClientComponent, ClusterClient}

class NetworkClientConfig {
  var clusterClient: ClusterClient = _
  var serviceName: String = _
  var zooKeeperConnectString: String = _
  var zooKeeperSessionTimeoutMillis = 30000

  var connectTimeoutMillis = 1000
  var writeTimeoutMillis = 100
  var maxConnectionsPerNode = 5
}

abstract class NettyNetworkClient(clientConfig: NetworkClientConfig) extends NetworkClient with ClusterClientComponent with NettyClusterIoClientComponent with MessageRegistryComponent {
  this: LoadBalancerFactoryComponent =>

  val messageRegistry = new MessageRegistry
  val clusterClient = if (clientConfig.clusterClient != null) clientConfig.clusterClient else ClusterClient(clientConfig.serviceName, clientConfig.zooKeeperConnectString,
    clientConfig.zooKeeperSessionTimeoutMillis)

  val executor = Executors.newCachedThreadPool
  val bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(executor, executor))
  val connectTimeoutMillis = clientConfig.connectTimeoutMillis
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
