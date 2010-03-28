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

import org.specs.SpecificationWithJUnit
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.group.{ChannelGroupFuture, ChannelGroup}
import org.specs.mock.Mockito
import org.jboss.netty.channel.{Channel, ChannelFutureListener, ChannelFuture}
import com.google.protobuf.Message
import java.util.concurrent.{TimeoutException, TimeUnit}
import java.net.InetSocketAddress

class ChannelPoolSpec extends SpecificationWithJUnit with Mockito {
  val channelGroup = mock[ChannelGroup]
  val bootstrap = mock[ClientBootstrap]
  val address = new InetSocketAddress("localhost", 31313)
  val channelPool = new ChannelPool(address, 1, 1, bootstrap, channelGroup)

  "ChannelPool" should {
    "close the ChannelGroup when close  is called" in {
      val future = mock[ChannelGroupFuture]
      channelGroup.close returns future
      future.awaitUninterruptibly returns future

      channelPool.close

      channelGroup.close was called
      future.awaitUninterruptibly was called
    }

    "throw a ChannelPoolClosedException if sendRequest is called after close is called" in {
      val future = mock[ChannelGroupFuture]
      channelGroup.close returns future
      future.awaitUninterruptibly returns future

      channelPool.close
      channelPool.sendRequest(mock[Request]) must throwA[ChannelPoolClosedException]
    }

    "open a new channel if no channels are open" in {
      val future = mock[ChannelFuture]
      bootstrap.connect(address) returns future

      channelPool.sendRequest(mock[Request])

      bootstrap.connect(address) was called
    }

    "not open a new channel if the max number of channels are already in the pool" in {
      val channel = mock[Channel]
      channel.isConnected returns true
      val future = new TestChannelFuture(channel, true)
      bootstrap.connect(address) returns future
      channelGroup.add(channel) returns true
      channel.write(any[Request]) returns future

      val request = Request(mock[Message], (e) => null)
      channelPool.sendRequest(request)
      future.listener.operationComplete(future)

      channelPool.sendRequest(mock[Request])

      channelGroup.add(channel) was called.once
      bootstrap.connect(address) was called.once
    }

    "open a new channel if the max number of channels are already in the pool but a channel is closed" in {
      val channel = mock[Channel]
      channel.isConnected returns false
      val future = new TestChannelFuture(channel, true)
      bootstrap.connect(address) returns future
      channelGroup.add(channel) returns true
      channel.write(any[Request]) returns future

      val request = Request(mock[Message], (e) => null)
      channelPool.sendRequest(request)
      future.listener.operationComplete(future)

      channelPool.sendRequest(request)
      future.listener.operationComplete(future)

      channelGroup.add(channel) was called.twice
      bootstrap.connect(address) was called.twice
    }

    "write all queued requests" in {
      val channel = mock[Channel]
      val request = mock[Request]
      request.timestamp returns System.currentTimeMillis + 10000
      channel.isConnected returns true
      val future = new TestChannelFuture(channel, true)
      bootstrap.connect(address) returns future
      channelGroup.add(channel) returns true
      channel.write(request) returns future

      channelPool.sendRequest(request)
      channelPool.sendRequest(request)
      channelPool.sendRequest(request)
      future.listener.operationComplete(future)

      channel.write(any[Request]) was called.times(3)
      bootstrap.connect(address) was called.once
    }

    "properly handle a failed write" in {
      val channel = mock[Channel]
      var either: Either[Throwable, Message] = null
      val request = spy(Request(mock[Message], (e) => either = e))
      request.timestamp returns System.currentTimeMillis + 10000
      channel.isConnected returns true
      val openFuture = new TestChannelFuture(channel, true)
      val writeFuture = new TestChannelFuture(channel, false)
      bootstrap.connect(address) returns openFuture
      channelGroup.add(channel) returns true
      channel.write(request) returns writeFuture

      channelPool.sendRequest(request)
      openFuture.listener.operationComplete(openFuture)
      writeFuture.listener.operationComplete(writeFuture)

      either must notBeNull
      either.isLeft must beTrue
    }

    "not write queued requests if the request timed out" in {
      val channel = mock[Channel]
      val goodRequest = spy(Request(mock[Message], (e) => null))
      var either: Either[Throwable, Message] = null
      val badRequest = spy(Request(mock[Message], (e) => either = e))
      goodRequest.timestamp returns System.currentTimeMillis + 10000
      badRequest.timestamp returns System.currentTimeMillis - 10000
      channel.isConnected returns true
      val future = new TestChannelFuture(channel, true)
      bootstrap.connect(address) returns future
      channelGroup.add(channel) returns true
      channel.write(goodRequest) returns future
      channel.write(badRequest) returns future

      channelPool.sendRequest(goodRequest)
      channelPool.sendRequest(badRequest)
      channelPool.sendRequest(goodRequest)
      future.listener.operationComplete(future)

      channel.write(goodRequest) was called.times(2)
      channel.write(badRequest) wasnt called
      bootstrap.connect(address) was called.once
      either must notBeNull
      either.isLeft must beTrue
      either.left.get must haveClass[TimeoutException]
    }

    "not write queued requests if the open failed" in {
      val channel = mock[Channel]
      val request = mock[Request]
      request.timestamp returns System.currentTimeMillis + 10000
      channel.isConnected returns true
      val future = new TestChannelFuture(channel, false)
      bootstrap.connect(address) returns future
      channelGroup.add(channel) returns true
      channel.write(request) returns future

      channelPool.sendRequest(request)
      channelPool.sendRequest(request)
      channelPool.sendRequest(request)
      future.listener.operationComplete(future)

      channel.write(any[Request]) wasnt called
      bootstrap.connect(address) was called.once
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
