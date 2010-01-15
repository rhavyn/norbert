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
import org.specs.util.WaitFor
import org.specs.mock.Mockito
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.network.{InvalidMessageException, MessageExecutorComponent, MessageRegistryComponent}
import java.net.SocketAddress
import org.jboss.netty.channel._
import java.util.UUID
import org.mockito.Matchers._
import com.google.protobuf.Message
import java.lang.{Exception, Integer}

class NettyRequestHandlerComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor
        with NettyRequestHandlerComponent with MessageRegistryComponent with MessageExecutorComponent {
  val messageRegistry = mock[MessageRegistry]
  val messageExecutor = new MessageExecutor(1, 1, 1) {
    var called = false
    override def executeMessage(message: Message, responseHandler: (Either[Exception, Message]) => Unit): Unit = called = true
  }
  val requestHandler = new NettyRequestHandler

  val event = mock[MessageEvent]
  val uuid = UUID.randomUUID
  val messageName = NorbertProtos.Ping.getDefaultInstance.getClass.getName
  val norbertMessageBuilder = NorbertProtos.NorbertMessage.newBuilder
          .setRequestIdMsb(uuid.getMostSignificantBits)
          .setRequestIdLsb(uuid.getLeastSignificantBits)
          .setMessageName(messageName)
  val pingBuilder = NorbertProtos.Ping.newBuilder

  "NettyRequestHandler" should {
    "when a message is received" in {
      "write back an error response if the recieved message is in the error state" in {
        norbertMessageBuilder.setStatus(NorbertProtos.NorbertMessage.Status.ERROR)
        val channel = new MyAbstractChannel {
          var msg: Any = _

          override def write(message: Any): ChannelFuture = {
            msg = message
            null
          }
        }
        event.getChannel returns channel
        event.getMessage returns norbertMessageBuilder.build

        requestHandler.messageReceived(null, event)

        channel.msg must haveClass[NorbertProtos.NorbertMessage]
        val message = channel.msg.asInstanceOf[NorbertProtos.NorbertMessage]
        message.getStatus must be_==(NorbertProtos.NorbertMessage.Status.ERROR)
        message.getMessageName must be_==(classOf[InvalidMessageException].getName)
        message.getRequestIdMsb must be_==(uuid.getMostSignificantBits)
        message.getRequestIdLsb must be_==(uuid.getLeastSignificantBits)
      }

      "write back an error response if the recieved message is not registered" in {
        norbertMessageBuilder.setStatus(NorbertProtos.NorbertMessage.Status.ERROR)
        val channel = new MyAbstractChannel {
          var msg: Any = _

          override def write(message: Any): ChannelFuture = {
            msg = message
            null
          }
        }
        event.getChannel returns channel

        event.getMessage returns norbertMessageBuilder.setMessage(pingBuilder.setTimestamp(1).build.toByteString).build
        messageRegistry.defaultInstanceForClassName(messageName) returns None

        requestHandler.messageReceived(null, event)

        channel.msg must haveClass[NorbertProtos.NorbertMessage]
        val message = channel.msg.asInstanceOf[NorbertProtos.NorbertMessage]
        message.getStatus must be_==(NorbertProtos.NorbertMessage.Status.ERROR)
        message.getMessageName must be_==(classOf[InvalidMessageException].getName)
        message.getRequestIdMsb must be_==(uuid.getMostSignificantBits)
        message.getRequestIdLsb must be_==(uuid.getLeastSignificantBits)
      }

      "calls the message executor with the message provided if registered" in {
        messageRegistry.defaultInstanceForClassName(messageName) returns Some(NorbertProtos.Ping.getDefaultInstance)
        event.getMessage returns norbertMessageBuilder.setMessage(pingBuilder.setTimestamp(1).build.toByteString).build
        event.getChannel returns mock[Channel]

        requestHandler.messageReceived(null, event)

        messageRegistry.defaultInstanceForClassName(messageName) was called
        messageExecutor.called must beTrue
      }
    }

    "when responseHandler is called" in {
      "write an error response if a Left(ex) is provided" in {
        val m = norbertMessageBuilder.setMessage(pingBuilder.setTimestamp(1).build.toByteString).build
        val channel = new MyAbstractChannel {
          var msg: Any = _

          override def write(p1: Any): ChannelFuture = {
            msg = p1
            null
          }
        }

        requestHandler.responseHandler(m, channel, Left(new Exception))

        channel.msg must haveClass[NorbertProtos.NorbertMessage]
        var message = channel.msg.asInstanceOf[NorbertProtos.NorbertMessage]
        message.getStatus must be_==(NorbertProtos.NorbertMessage.Status.ERROR)
        message.getMessageName must be_==(classOf[Exception].getName)
        message.getErrorMessage must be_==("")
        message.getRequestIdMsb must be_==(uuid.getMostSignificantBits)
        message.getRequestIdLsb must be_==(uuid.getLeastSignificantBits)

        val exceptionMessage = "An exception message"
        requestHandler.responseHandler(m, channel, Left(new Exception(exceptionMessage)))

        channel.msg must haveClass[NorbertProtos.NorbertMessage]
        message = channel.msg.asInstanceOf[NorbertProtos.NorbertMessage]
        message.getStatus must be_==(NorbertProtos.NorbertMessage.Status.ERROR)
        message.getMessageName must be_==(classOf[Exception].getName)
        message.getErrorMessage must be_==(exceptionMessage)
        message.getRequestIdMsb must be_==(uuid.getMostSignificantBits)
        message.getRequestIdLsb must be_==(uuid.getLeastSignificantBits)
      }

      "write a response if a Right(message) is provided" in {
        val ping = NorbertProtos.Ping.newBuilder.setTimestamp(1001).build
        val m = norbertMessageBuilder.setMessage(pingBuilder.setTimestamp(1).build.toByteString).build
        val channel = new MyAbstractChannel {
          var msg: Any = _

          override def write(p1: Any): ChannelFuture = {
            msg = p1
            null
          }
        }

        requestHandler.responseHandler(m, channel, Right(ping))

        channel.msg must haveClass[NorbertProtos.NorbertMessage]
        var message = channel.msg.asInstanceOf[NorbertProtos.NorbertMessage]
        message.getStatus must be_==(NorbertProtos.NorbertMessage.Status.OK)
        message.getMessageName must be_==(ping.getClass.getName)
        message.getMessage must be_==(ping.toByteString)
        message.getRequestIdMsb must be_==(uuid.getMostSignificantBits)
        message.getRequestIdLsb must be_==(uuid.getLeastSignificantBits)
      }
    }
  }

  private class MyAbstractChannel extends Channel {
    def compareTo(p1: Channel): Int = 0

    def setReadable(p1: Boolean): ChannelFuture = null

    def setInterestOps(p1: Int): ChannelFuture = null

    def isWritable: Boolean = false

    def isReadable: Boolean = false

    def getInterestOps: Int = 0

    def getCloseFuture: ChannelFuture = null

    def close: ChannelFuture = null

    def unbind: ChannelFuture = null

    def disconnect: ChannelFuture = null

    def connect(p1: SocketAddress): ChannelFuture = null

    def bind(p1: SocketAddress): ChannelFuture = null

    def write(p1: Any, p2: SocketAddress): ChannelFuture = null

    def write(p1: Any): ChannelFuture = null

    def getRemoteAddress: SocketAddress = null

    def getLocalAddress: SocketAddress = null

    def isConnected: Boolean = false

    def isBound: Boolean = false

    def isOpen: Boolean = false

    def getPipeline: ChannelPipeline = null

    def getConfig: ChannelConfig = null

    def getParent: Channel = null

    def getFactory: ChannelFactory = null

    def getId: Integer = null
  }
//  "ChannelHandlerActor" should {
//    "for MessageReceived" in {
//      "invoke the message handler and, if there is a response message, send it to the client" in {
//        val message = NorbertProtos.NorbertMessage.getDefaultInstance
//        requestHandler.handleMessage(message) returns Some(message)
//        val channel =  mock[Channel]
//        channel.write(message) returns mock[ChannelFuture]
//        val event = mock[MessageEvent]
//        event.getMessage returns message
//
//        val actor = new ChannelHandlerActor(channel)
//        actor.start
//        actor ! NettyMessages.MessageReceived(null, event)
//        waitFor(10.ms)
//
//        requestHandler.handleMessage(message) was called
//        channel.write(message) was called
//      }
//
//      "invoke the message handler and, if there isn't a response message, do nothing" in {
//        val message = NorbertProtos.NorbertMessage.getDefaultInstance
//        requestHandler.handleMessage(message) returns None
//        val channel =  mock[Channel]
//        val event = mock[MessageEvent]
//        event.getMessage returns message
//
//        val actor = new ChannelHandlerActor(channel)
//        actor.start
//        actor ! NettyMessages.MessageReceived(null, event)
//        waitFor(10.ms)
//
//        requestHandler.handleMessage(message) was called
//        channel.write(isA(classOf[NorbertProtos.NorbertMessage])) wasnt called
//      }
//    }
//  }
}
