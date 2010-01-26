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

import java.net.InetSocketAddress
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.util.Logging
import com.linkedin.norbert.cluster.{ClusterComponent, InvalidNodeException, Node}
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import org.jboss.netty.channel.{ChannelException, Channel, ChannelPipelineFactory, Channels}
import com.linkedin.norbert.network._

trait NettyNetworkServerComponent extends NetworkServerComponent {
  this: BootstrapFactoryComponent with ClusterComponent with NettyRequestHandlerComponent with NetworkClientFactoryComponent with MessageExecutorComponent =>

  class NettyNetworkServer private (nodeIdOption: Option[Int], bindAddressOption: Option[InetSocketAddress]) extends NetworkServer with ClusterListener with Logging {
    def this(nodeId: Int) = this(Some(nodeId), None)
    def this(bindAddress: InetSocketAddress) = this(None, Some(bindAddress))

    private val bootstrap = bootstrapFactory.newServerBootstrap
    bootstrap.setPipelineFactory(pipelineFactory)
    bootstrap.setOption("child.tcpNoDelay", true)
    bootstrap.setOption("child.reuseAddress", true)
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("reuseAddress", true)

    private var serverChannel: Channel = _
    private var myNode: Node = _

    @volatile private var markAvailableWhenConnected = true

    def bind {
      log.ifInfo("Starting network server...")
      log.ifDebug("Starting cluster...")
      cluster.start
      log.ifDebug("Cluster started")
      
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

      log.ifInfo("Network server started and listening at %s", bindAddress)

      cluster.addListener(this)
    }

    def bind(markAvailable: Boolean) {
      markAvailableWhenConnected = markAvailable
      bind
    }

    def markAvailable = {
      if (myNode == null) throw new NetworkingException("bind() must be called before calling markAvailable")
      markAvailableWhenConnected = true
      doMarkNodeAvailable
    }

    def currentNode = myNode

    def shutdown {
      log.info("Shutting down network server...")
      log.ifDebug("Shutting down cluster...")
      cluster.shutdown

      if (serverChannel != null) {
        log.ifDebug("Unbinding from %s...", serverChannel.getLocalAddress)
        val future = serverChannel.close
        future.awaitUninterruptibly
      }

      log.ifDebug("Shutting down MessageExecutor...")
      messageExecutor.shutdown

      log.ifDebug("Shutting down NettyRequestHandler...")
      requestHandler.shutdown
      bootstrap.releaseExternalResources

      log.ifDebug("Shutting down NetworkClientFactory...")
      networkClientFactory.shutdown

      log.info("Network server shut down")
    }

    def handleClusterEvent(event: ClusterEvent) = event match {
      case ClusterEvents.Connected(_, _) => doMarkNodeAvailable

      case _ => // do nothing
    }

    protected def pipelineFactory = new ChannelPipelineFactory {
      def getPipeline = {
        val p = Channels.pipeline
        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Math.MAX_INT, 0, 4, 0, 4))
        p.addLast("protobufDecoder", new ProtobufDecoder(NorbertProtos.NorbertMessage.getDefaultInstance))

        p.addLast("frameEncoder", new LengthFieldPrepender(4))
        p.addLast("protobufEncoder", new ProtobufEncoder)

        p.addLast("requestHandler", requestHandler)

        p
      }
    }

    private def doMarkNodeAvailable {
      if (markAvailableWhenConnected) {
        log.ifDebug("Marking node with id %d available", myNode.id)
        cluster.markNodeAvailable(myNode.id)
      }
    }
  }
}
