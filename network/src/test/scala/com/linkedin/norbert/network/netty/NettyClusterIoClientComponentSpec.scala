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
import java.util.concurrent.{TimeoutException, TimeUnit}
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.group.{ChannelGroupFuture, ChannelGroup}
import org.jboss.netty.channel.{Channel, ChannelFutureListener, ChannelFuture, ChannelPipelineFactory}
import org.mockito.Matchers._
import org.specs.mock.Mockito
import org.specs.SpecificationWithJUnit
import org.specs.util.WaitFor
import com.google.protobuf.Message
import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.{NetworkDefaults, Request, MessageRegistryComponent}

class NettyClusterIoClientComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with NettyClusterIoClientComponent
        with BootstrapFactoryComponent with RequestHandlerComponent with MessageRegistryComponent {
  val clusterIoClient = mock[ClusterIoClient]
  val bootstrapFactory = mock[BootstrapFactory]
  val messageRegistry = null
  
  "NettyClusterIoClient" should {
    "correctly configure the ClientBootstrap" in {
      val bootstrap = mock[ClientBootstrap]
      bootstrapFactory.newClientBootstrap returns bootstrap
      doNothing.when(bootstrap).setPipelineFactory(isA(classOf[ChannelPipelineFactory]))
      doNothing.when(bootstrap).setOption("tcpNoDelay", true)
      doNothing.when(bootstrap).setOption("reuseAddress", true)

      new NettyClusterIoClient(mock[ChannelGroup])

      bootstrap.setPipelineFactory(isA(classOf[ChannelPipelineFactory])) was called
      bootstrap.setOption("tcpNoDelay", true) was called
      bootstrap.setOption("reuseAddress", true) was called
    }

    "when sendRequest is called" in {
      "opens a Channel to a node if necessary and sends a request if the open succeeds" in {
        val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0), true)
        val bootstrap = mock[ClientBootstrap]
        bootstrapFactory.newClientBootstrap returns bootstrap
        val channel = mock[Channel]
        val future = new TestChannelFuture(channel, true)
        bootstrap.connect(node.address) returns future
        channel.write(isA(classOf[Request])) returns mock[ChannelFuture]

        val clusterIoClient = new NettyClusterIoClient(mock[ChannelGroup])
        clusterIoClient.sendRequest(Set(node), Request(mock[Message], 1))
        future.listener.operationComplete(future)

        bootstrap.connect(node.address) was called
        channel.write(isA(classOf[Request])) was called
      }

      "does not send a request if unable to open a Channel" in {
        val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0), true)
        val bootstrap = mock[ClientBootstrap]
        bootstrapFactory.newClientBootstrap returns bootstrap
        val channel = mock[Channel]
        val future = new TestChannelFuture(channel, false)
        bootstrap.connect(node.address) returns future

        val clusterIoClient = new NettyClusterIoClient(mock[ChannelGroup])
        val request = Request(mock[Message], 1)
        clusterIoClient.sendRequest(Set(node), request)
        future.listener.operationComplete(future)

        bootstrap.connect(node.address) was called
        channel.write(isA(classOf[Request])) wasnt called
        request.responseIterator.next(1, TimeUnit.MILLISECONDS) must beSome[Either[Throwable, Message]].which(_.isLeft)
      }

      "handles the exception if unable to write request" in {
        val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0), true)
        val bootstrap = mock[ClientBootstrap]
        bootstrapFactory.newClientBootstrap returns bootstrap
        val channel = mock[Channel]
        val openFuture = new TestChannelFuture(channel, true)
        val writeFuture = new TestChannelFuture(channel, false)
        bootstrap.connect(node.address) returns openFuture
        channel.write(isA(classOf[Request])) returns writeFuture

        val clusterIoClient = new NettyClusterIoClient(mock[ChannelGroup])
        val request = Request(mock[Message], 1)
        clusterIoClient.sendRequest(Set(node), request)
        openFuture.listener.operationComplete(openFuture)
        writeFuture.listener.operationComplete(writeFuture)

        bootstrap.connect(node.address) was called
        channel.write(isA(classOf[Request])) was called
        request.responseIterator.next(1, TimeUnit.MILLISECONDS) must beSome[Either[Throwable, Message]].which(_.isLeft)
      }

      "doesn't opens a Channel to a node if a channel is available in the pool" in {
        val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0), true)
        val bootstrap = mock[ClientBootstrap]
        bootstrapFactory.newClientBootstrap returns bootstrap
        val channel = mock[Channel]
        channel.isOpen returns true
        val future = new TestChannelFuture(channel, true)
        bootstrap.connect(node.address) returns future
        channel.write(isA(classOf[Request])) returns mock[ChannelFuture]

        val clusterIoClient = new NettyClusterIoClient(mock[ChannelGroup])
        clusterIoClient.sendRequest(Set(node), Request(mock[Message], 1))
        future.listener.operationComplete(future)
        clusterIoClient.sendRequest(Set(node), Request(mock[Message], 1))

        bootstrap.connect(node.address) was called.once
        channel.write(isA(classOf[Request])) was called.twice
      }

      "discards a closed Channel in the pool and opens a new one" in {
        val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0), true)
        val bootstrap = mock[ClientBootstrap]
        bootstrapFactory.newClientBootstrap returns bootstrap
        val channel = mock[Channel]
        channel.isOpen returns false
        val future = new TestChannelFuture(channel, true)
        bootstrap.connect(node.address) returns future
        channel.write(isA(classOf[Request])) returns mock[ChannelFuture]

        val clusterIoClient = new NettyClusterIoClient(mock[ChannelGroup])
        clusterIoClient.sendRequest(Set(node), Request(mock[Message], 1))
        future.listener.operationComplete(future)
        clusterIoClient.sendRequest(Set(node), Request(mock[Message], 1))
        future.listener.operationComplete(future)

        bootstrap.connect(node.address) was called.twice
        channel.write(isA(classOf[Request])) was called.twice
      }

      "only opens maxConnectionsPerNode channels" in {
        val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0), true)
        val bootstrap = mock[ClientBootstrap]
        bootstrapFactory.newClientBootstrap returns bootstrap
        val channel = mock[Channel]
        channel.isOpen returns true
        val future = new TestChannelFuture(channel, true)
        bootstrap.connect(node.address) returns future
        channel.write(isA(classOf[Request])) returns mock[ChannelFuture]

        val clusterIoClient = new NettyClusterIoClient(1, NetworkDefaults.WRITE_TIMEOUT, mock[ChannelGroup])
        clusterIoClient.sendRequest(Set(node), Request(mock[Message], 1))
        clusterIoClient.sendRequest(Set(node), Request(mock[Message], 1))
        future.listener.operationComplete(future)

        bootstrap.connect(node.address) was called.once
        channel.write(isA(classOf[Request])) was called.twice
      }

      "if the write timeout has elapsed it doesn't write when opening a new channel" in {
        val node = Node(1, new InetSocketAddress("localhost", 31313), Array(0), true)
        val bootstrap = mock[ClientBootstrap]
        bootstrapFactory.newClientBootstrap returns bootstrap
        val channel = mock[Channel]
        channel.isOpen returns true
        val future = new TestChannelFuture(channel, true)
        bootstrap.connect(node.address) returns future
        channel.write(isA(classOf[Request])) returns mock[ChannelFuture]

        val clusterIoClient = new NettyClusterIoClient(NetworkDefaults.MAX_CONNECTIONS_PER_NODE, 1, mock[ChannelGroup])
        val request = Request(mock[Message], 1)
        clusterIoClient.sendRequest(Set(node), request)
        waitFor(2.ms)
        future.listener.operationComplete(future)

        bootstrap.connect(node.address) was called
        channel.write(isA(classOf[Request])) wasnt called
        request.responseIterator.next(1, TimeUnit.MILLISECONDS) must beSome[Either[Throwable, Message]].which { either =>
          either.isLeft must beTrue
          either.left.get must haveClass[TimeoutException]
        }
      }

      "shutdown cleans up the ChannelGroup and ClientBootstrap" in {
        val bootstrap = mock[ClientBootstrap]
        bootstrapFactory.newClientBootstrap returns bootstrap
        val channelGroup = mock[ChannelGroup]
        doNothing.when(bootstrap).releaseExternalResources
        channelGroup.close returns mock[ChannelGroupFuture]

        new NettyClusterIoClient(1, 1, channelGroup).shutdown

        bootstrap.releaseExternalResources was called
        channelGroup.close was called
      }
    }
  }

  class TestChannelFuture(channel: Channel, success: Boolean) extends ChannelFuture {
    var listener: ChannelFutureListener = _

    def getCause = new Exception

    def awaitUninterruptibly(timeout: Long, unit: TimeUnit) = false

    def awaitUninterruptibly = null

    def isDone = false

    def await = null

    def isCancelled = false

    def addListener(l: ChannelFutureListener) = listener = l

    def await(timeout: Long, unit: TimeUnit) = false

    def isSuccess = success

    def getChannel = channel

    def await(timeoutMillis: Long) = false

    def removeListener(listener: ChannelFutureListener) = {}

    def awaitUninterruptibly(timeoutMillis: Long) = false

    def setFailure(cause: Throwable) = false

    def cancel = false

    def setSuccess = false
  }
}
