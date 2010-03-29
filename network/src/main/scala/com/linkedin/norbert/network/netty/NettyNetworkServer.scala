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

import com.linkedin.norbert.cluster.{ClusterClient, ClusterClientComponent}
import java.util.concurrent.Executors
import com.linkedin.norbert.util.NamedPoolThreadFactory
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import com.linkedin.norbert.protos.NorbertProtos
import org.jboss.netty.channel.group.DefaultChannelGroup
import com.linkedin.norbert.network.server.{NetworkServer, MessageHandlerRegistry, ThreadPoolMessageExecutor, MessageHandlerRegistryComponent}

class NetworkServerConfig {
  var clusterClient: ClusterClient = _
  var serviceName: String = _
  var zooKeeperConnectString: String = _
  var zooKeeperSessionTimeoutMillis = 30000

  var requestThreadCorePoolSize = 5
  var requestThreadMaxPoolSize = 10
  var requestThreadKeepAliveTimeSeconds = 5
}

class NettyNetworkServer(serverConfig: NetworkServerConfig) extends NetworkServer with ClusterClientComponent with NettyClusterIoServerComponent with MessageHandlerRegistryComponent {
  val clusterClient = if (serverConfig.clusterClient != null) serverConfig.clusterClient else ClusterClient(serverConfig.serviceName, serverConfig.zooKeeperConnectString,
    serverConfig.zooKeeperSessionTimeoutMillis)

  val messageHandlerRegistry = new MessageHandlerRegistry
  val messageExecutor = new ThreadPoolMessageExecutor(messageHandlerRegistry, serverConfig.requestThreadCorePoolSize, serverConfig.requestThreadMaxPoolSize,
    serverConfig.requestThreadKeepAliveTimeSeconds)

  val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("norbert-server-pool-%s".format(clusterClient.serviceName)))
  val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(executor, executor))
  bootstrap.setOption("reuseAddress", true)
  bootstrap.setOption("tcpNoDelay", true)
  bootstrap.setOption("child.tcpNoDelay", true)
  bootstrap.setOption("child.reuseAddress", true)
  val p = bootstrap.getPipeline
  p.addFirst("logging", new LoggingHandler)

  p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Math.MAX_INT, 0, 4, 0, 4))
  p.addLast("protobufDecoder", new ProtobufDecoder(NorbertProtos.NorbertMessage.getDefaultInstance))

  p.addLast("frameEncoder", new LengthFieldPrepender(4))
  p.addLast("protobufEncoder", new ProtobufEncoder)

  val channelGroup = new DefaultChannelGroup("norbert-server-group-%s".format(clusterClient.serviceName))
  p.addLast("requestHandler", new ServerChannelHandler(channelGroup, messageHandlerRegistry, messageExecutor))

  val clusterIoServer = new NettyClusterIoServer(bootstrap, channelGroup)

  override def shutdown = if (serverConfig.clusterClient == null) clusterClient.shutdown else super.shutdown
}
