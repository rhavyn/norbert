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
package com.linkedin.norbert.network

import netty.{ServerChannelHandler, NettyClusterIoServerComponent}
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.bootstrap.ServerBootstrap
import java.util.concurrent.Executors
import org.jboss.netty.logging.{InternalLoggerFactory, Log4JLoggerFactory}
import com.linkedin.norbert.cluster.{ClusterClientComponent}
import com.linkedin.norbert.cluster.zookeeper.ZooKeeperClusterClient
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import com.linkedin.norbert.protos.NorbertProtos
import server._
import org.jboss.netty.channel.group.DefaultChannelGroup
import com.google.protobuf.Message
import org.jboss.netty.handler.logging.LoggingHandler
import com.linkedin.norbert.util.NamedPoolThreadFactory

object NorbertNetworkServerMain {
  InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)

  def main(args: Array[String]) {
    val cc = new ZooKeeperClusterClient("localhost:2181", 30000, "nimbus")
    cc.start
    cc.awaitConnectionUninterruptibly
    cc.removeNode(1)
    cc.addNode(1, "localhost:31313", new Array[Int](0))

    val ns = new NetworkServer with ClusterClientComponent with NettyClusterIoServerComponent
        with MessageHandlerRegistryComponent {
      val clusterClient = cc
      val messageHandlerRegistry = new MessageHandlerRegistry
      val messageExecutor = new ThreadPoolMessageExecutor(messageHandlerRegistry, 5, 10, 1000)

      val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("norbert-server-pool-%s".format(cc.serviceName)))
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

      val channelGroup = new DefaultChannelGroup("norbert-server-group-%s".format(cc.serviceName))
      p.addLast("requestHandler", new ServerChannelHandler(channelGroup, messageHandlerRegistry, messageExecutor))

      val clusterIoServer = new NettyClusterIoServer(bootstrap, channelGroup)
    }

    ns.registerHandler(NorbertProtos.Ping.getDefaultInstance, NorbertProtos.PingResponse.getDefaultInstance, pingHandler _)

    ns.bind(1)

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run = {
        ns.shutdown
      }
    })
  }

  private def pingHandler(message: Message): Message = {
    val ping = message.asInstanceOf[NorbertProtos.Ping]
    NorbertProtos.PingResponse.newBuilder.setTimestamp(ping.getTimestamp).build
  }
}
