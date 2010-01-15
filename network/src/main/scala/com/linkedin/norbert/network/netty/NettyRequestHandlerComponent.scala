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

import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.util.Logging
import org.jboss.netty.channel._
import com.google.protobuf.Message
import com.linkedin.norbert.network.{InvalidMessageException, MessageRegistryComponent, MessageExecutorComponent}

trait NettyRequestHandlerComponent {
  this: MessageExecutorComponent with MessageRegistryComponent =>

  val requestHandler: NettyRequestHandler

  @ChannelPipelineCoverage("all")
  class NettyRequestHandler extends SimpleChannelHandler with Logging {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      log.ifDebug("MessageReceived (%s): %s", e.getChannel, e.getMessage)

      val channel = e.getChannel
      val norbertMessage = e.getMessage.asInstanceOf[NorbertProtos.NorbertMessage]

      if (norbertMessage.getStatus != NorbertProtos.NorbertMessage.Status.OK) {
        log.warn("Received invalid message: %s", norbertMessage)
        channel.write(newErrorMessage(norbertMessage, new InvalidMessageException("Recieved a request in the error state")))
      } else {
        messageRegistry.defaultInstanceForClassName(norbertMessage.getMessageName) match {
          case Some(defaultInstance) =>
            val message = defaultInstance.newBuilderForType.mergeFrom(norbertMessage.getMessage).build
            messageExecutor.executeMessage(message, either => responseHandler(norbertMessage, channel, either))

          case None =>
            val errorMsg = "Received a request without a registered message type: %s".format(norbertMessage.getMessageName)
            log.warn(errorMsg)
            channel.write(newErrorMessage(norbertMessage, new InvalidMessageException(errorMsg)))
        }
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.info(e.getCause, "Caught exception")

    def responseHandler(norbertMessage: NorbertProtos.NorbertMessage, channel: Channel, either: Either[Exception, Message]) {
      val message = either match {
        case Left(ex) => newErrorMessage(norbertMessage, ex)
        case Right(message) => newBuilder(norbertMessage).setMessageName(message.getClass.getName).setMessage(message.toByteString).build
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
      newBuilder(norbertMessage).setMessageName(ex.getClass.getName)
            .setStatus(NorbertProtos.NorbertMessage.Status.ERROR)
            .setErrorMessage(if (ex.getMessage == null) "" else ex.getMessage)
            .build
    }
  }
}
