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
package com.linkedin.norbert
package network
package netty

import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.channel._
import com.google.protobuf.{InvalidProtocolBufferException, Message}
import server.{MessageExecutor, MessageHandlerRegistry}
import protos.NorbertProtos
import logging.Logging

@ChannelPipelineCoverage("all")
class ServerChannelHandler(channelGroup: ChannelGroup, messageHandlerRegistry: MessageHandlerRegistry, messageExecutor: MessageExecutor) extends SimpleChannelHandler with Logging {
  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val channel = e.getChannel
    log.ifTrace("channelOpen: " + channel)
    channelGroup.add(channel)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val channel = e.getChannel
    val norbertMessage = e.getMessage.asInstanceOf[NorbertProtos.NorbertMessage]
    log.ifTrace("messageRecieved [%s]: %s", channel, norbertMessage)

    if (norbertMessage.getStatus != NorbertProtos.NorbertMessage.Status.OK) {
      log.warn("Received invalid message: %s", norbertMessage)
      channel.write(newErrorMessage(norbertMessage, new InvalidMessageException("Recieved a request in the error state")))
    } else {
      try {
        val di = messageHandlerRegistry.requestMessageDefaultInstanceFor(norbertMessage.getMessageName)
        val message = di.newBuilderForType.mergeFrom(norbertMessage.getMessage).build
        log.ifDebug("Queuing to MessageExecutor: %s", message)
        messageExecutor.executeMessage(message, either => responseHandler(norbertMessage, channel, either))
      } catch {
        case ex: InvalidMessageException => log.error(ex, "Recieved invalid message")
        case ex: InvalidProtocolBufferException => log.error(ex, "Error deserializing message")
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.info(e.getCause, "Caught exception in network layer")

  def responseHandler(norbertMessage: NorbertProtos.NorbertMessage, channel: Channel, either: Either[Exception, Message]) {
    val message = either match {
      case Left(ex) => newErrorMessage(norbertMessage, ex)
      case Right(message) => newBuilder(norbertMessage).
          setMessageName(message.getDescriptorForType.getFullName).
          setMessage(message.toByteString).
          build
    }

    log.ifDebug("Sending response: %s", message)

    channel.write(message)
  }

  private def newBuilder(norbertMessage: NorbertProtos.NorbertMessage) = {
    val builder = NorbertProtos.NorbertMessage.newBuilder()
    builder.setRequestIdMsb(norbertMessage.getRequestIdMsb)
    builder.setRequestIdLsb(norbertMessage.getRequestIdLsb)
    builder
  }

  private def newErrorMessage(norbertMessage: NorbertProtos.NorbertMessage, ex: Exception) = {
    newBuilder(norbertMessage).setMessageName(ex.getClass.getName).
        setStatus(NorbertProtos.NorbertMessage.Status.ERROR).
        setErrorMessage(if (ex.getMessage == null) "" else ex.getMessage).
        build
  }
}
