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

import com.google.protobuf.Message
import com.linkedin.norbert.cluster.Node
import org.jboss.netty.bootstrap.ClientBootstrap
import java.net.InetSocketAddress
import java.util.concurrent.{Executors, ConcurrentHashMap}
import com.linkedin.norbert.util.{NamedPoolThreadFactory, Logging}
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import com.linkedin.norbert.network.common.{MessageRegistryComponent, ClusterIoClientComponent}
import org.jboss.netty.handler.codec.protobuf.{ProtobufEncoder, ProtobufDecoder}
import org.jboss.netty.handler.codec.frame.{LengthFieldPrepender, LengthFieldBasedFrameDecoder}
import com.linkedin.norbert.protos.NorbertProtos
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.channel.{Channels, ChannelFactory, ChannelPipelineFactory}

/**
 * A <code>ClusterIoClientComponent</code> implementation that uses Netty for network communication.
 */
trait NettyClusterIoClientComponent extends ClusterIoClientComponent with ChannelPoolComponent {
  this: MessageRegistryComponent =>

  object NettyClusterIoClient {
    private val counter = new AtomicInteger

    def channelFactory = {
      val pool = Executors.newCachedThreadPool(new NamedPoolThreadFactory("norbert-client-%d".format(counter.incrementAndGet)))
      new NioClientSocketChannelFactory(pool, pool)
    }
  }

  class NettyClusterIoClient(connectTimeoutMillis: Int, channelPoolFactory: ChannelPoolFactory, channelFactory: ChannelFactory)
      (implicit bootstrapFactory: (ChannelFactory, InetSocketAddress, Int) => ClientBootstrap) extends ClusterIoClient with UrlParser with Logging {
    def this(connectTimeoutMillis: Int, channelPoolFactory: ChannelPoolFactory)
        (implicit bootstrapFactory: (ChannelFactory, InetSocketAddress, Int) => ClientBootstrap) = this(connectTimeoutMillis, channelPoolFactory,
      NettyClusterIoClient.channelFactory)(bootstrapFactory)

    private val channelPools = new ConcurrentHashMap[Node, ChannelPool]

    def sendMessage(node: Node, message: Message, responseCallback: (Either[Throwable, Message]) => Unit) = {
      if (node == null || message == null || responseCallback == null) throw new NullPointerException

      var pool = channelPools.get(node)
      if (pool == null) {
        val (address, port) = parseUrl(node.url)
        val bootstrap = bootstrapFactory(channelFactory, new InetSocketAddress(address, port), connectTimeoutMillis)
        bootstrap.setPipelineFactory(new NorbertChannelPipelineFactory)

        pool = channelPoolFactory.newChannelPool(bootstrap)
        channelPools.putIfAbsent(node, pool)
        pool = channelPools.get(node)
      }

      pool.sendRequest(Request(message, responseCallback))
    }

    def shutdown = {
      import scala.collection.jcl.Conversions._

      channelPools.keySet.foreach { key =>
        channelPools.get(key) match {
          case null => // do nothing
          case pool =>
            pool.close
            channelPools.remove(key)
        }
      }

      log.ifDebug("NettyClusterIoClient shut down")
    }
  }

  protected implicit def defaultBootstrapFactory(channelFactory: ChannelFactory, remoteAddress: InetSocketAddress, connectTimeoutMillis: Int) = {
    val bootstrap = new ClientBootstrap(channelFactory)
    bootstrap.setOption("remoteAddress", remoteAddress)
    bootstrap.setOption("connectTimeoutMillis", connectTimeoutMillis)
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("reuseAddress", true)
    bootstrap.setPipelineFactory(new NorbertChannelPipelineFactory)

    bootstrap
  }

  private class NorbertChannelPipelineFactory extends ChannelPipelineFactory {
    val p = Channels.pipeline
    p.addFirst("logging", new LoggingHandler)

    p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Math.MAX_INT, 0, 4, 0, 4))
    p.addLast("protobufDecoder", new ProtobufDecoder(NorbertProtos.NorbertMessage.getDefaultInstance))

    p.addLast("frameEncoder", new LengthFieldPrepender(4))
    p.addLast("protobufEncoder", new ProtobufEncoder)

    p.addLast("requestHandler", new ClientChannelHandler(messageRegistry))

    def getPipeline = p
  }
}
