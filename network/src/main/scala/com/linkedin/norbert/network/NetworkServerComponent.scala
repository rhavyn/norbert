/*
 * Copyright 2009 LinkedIn, Inc
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

import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.protobuf.{ProtobufEncoder, ProtobufDecoder}
import org.jboss.netty.handler.codec.frame.{LengthFieldPrepender, LengthFieldBasedFrameDecoder}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory, ChannelException, Channel}
import org.jboss.netty.channel.group.DefaultChannelGroup
import com.linkedin.norbert.util.Logging
import netty.{ChannelHandlerActorAdapter, ChannelHandlerActorComponent}
import com.linkedin.norbert.cluster.{Node, InvalidNodeException, ClusterComponent}
import com.linkedin.norbert.protos.NorbertProtos

trait NetworkServerComponent {
  this: BootstrapFactoryComponent with ClusterComponent with ChannelHandlerActorComponent with NetworkClientFactoryComponent =>

  val networkServer: NetworkServer
  
  class NetworkServer private (nodeIdOption: Option[Int], bindAddressOption: Option[InetSocketAddress]) extends Logging {
    def this(nodeId: Int) = this(Some(nodeId), None)
    def this(bindAddress: InetSocketAddress) = this(None, Some(bindAddress))

    private val bootstrap = bootstrapFactory.newServerBootstrap
    bootstrap.setPipelineFactory(pipelineFactory)
    bootstrap.setOption("child.tcpNoDelay", true)
    bootstrap.setOption("child.reuseAddress", true)
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("reuseAddress", true)

    private var serverChannel: Channel = _
    private val channelGroup = new DefaultChannelGroup("norbert-server")
    private var myNode: Node = _

    def bind {
      log.ifDebug("Waiting for cluster connection to complete...")
      cluster.awaitConnectionUninterruptibly

      val bindAddress = if (nodeIdOption.isDefined) {
        val nodeId = nodeIdOption.get
        log.ifDebug("Binding network server for node with id: " + nodeId)
        myNode = cluster.nodeWithId(nodeId).getOrElse(throw new InvalidNodeException("Unable to find a node with id: %d".format(nodeId)))
        myNode.address
      } else {
        val address = bindAddressOption.get
        log.ifDebug("Binding network server for node with address: " + address)
        myNode = cluster.nodeWithAddress(address).getOrElse(throw new InvalidNodeException("Unable to find a node with address: %s".format(address)))
        address
      }

      try {
        serverChannel = bootstrap.bind(bindAddress)
      } catch {
        case ex: ChannelException => throw new NetworkingException("Unable to bind to port", ex)
      }

      log.info("Listening at %s", bindAddress)
      
      cluster.addListener(new ClusterListener {
        def handleClusterEvent(event: ClusterEvent) = event match {
          case ClusterEvents.Connected(_, _) =>
            log.info("Marking node with id %d available", myNode.id)
            cluster.markNodeAvailable(myNode.id)
          case _ => // do nothing
        }
      })
    }

    def currentNode: Node = myNode

    def shutdown {
      cluster.shutdown

      if (serverChannel != null) {
        log.info("Shutting down network server listening at %s...", serverChannel.getLocalAddress)
        serverChannel.close
        serverChannel.getCloseFuture.awaitUninterruptibly
      }
      val future = channelGroup.close
      future.awaitUninterruptibly
      bootstrap.releaseExternalResources

      networkClientFactory.shutdown
      
      log.info("Shutdown complete")
    }

    protected def pipelineFactory = new ChannelPipelineFactory {
      def getPipeline = {
        val p = Channels.pipeline
        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Math.MAX_INT, 0, 4, 0, 4))
        p.addLast("protobufDecoder", new ProtobufDecoder(NorbertProtos.NorbertMessage.getDefaultInstance))

        p.addLast("frameEncoder", new LengthFieldPrepender(4))
        p.addLast("protobufEncoder", new ProtobufEncoder)

        p.addLast("channelHandler", new ChannelHandlerActorAdapter(channelGroup, channel => new ChannelHandlerActor(channel)))
        
        p
      }
    }
  }
}
