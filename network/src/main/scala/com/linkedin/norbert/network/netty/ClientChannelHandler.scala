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

import com.linkedin.norbert.util.Logging
import java.util.concurrent.ConcurrentHashMap
import java.util.UUID
import com.linkedin.norbert.protos.NorbertProtos
import org.jboss.netty.channel._
import com.linkedin.norbert.network.RemoteException
import com.google.protobuf.InvalidProtocolBufferException
import com.linkedin.norbert.network.common.MessageRegistry

@ChannelPipelineCoverage("all")
class ClientChannelHandler(messageRegistry: MessageRegistry) extends SimpleChannelHandler with Logging {
  private val requestMap = new ConcurrentHashMap[UUID, Request]

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val request = e.getMessage.asInstanceOf[Request]
    log.ifDebug("Writing request: %s", request)

    if (messageRegistry.hasResponse(request.message)) requestMap.put(request.id, request)

    val message = NorbertProtos.NorbertMessage.newBuilder
    message.setRequestIdMsb(request.id.getMostSignificantBits)
    message.setRequestIdLsb(request.id.getLeastSignificantBits)
    message.setMessageName(request.message.getDescriptorForType.getFullName)
    message.setMessage(request.message.toByteString)

    super.writeRequested(ctx, new DownstreamMessageEvent(e.getChannel, e.getFuture, message.build, e.getRemoteAddress))
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val message = e.getMessage.asInstanceOf[NorbertProtos.NorbertMessage]
    log.ifDebug("Received message: %s", message)
    val requestId = new UUID(message.getRequestIdMsb, message.getRequestIdLsb)

    requestMap.get(requestId) match {
      case null => log.warn("Received a response message [%s] without a corresponding request", message)
      case request =>
        if (message.getStatus == NorbertProtos.NorbertMessage.Status.OK) {
          try {
            val rdi = messageRegistry.responseMessageDefaultInstanceFor(request.message)
            request.responseCallback(Right(rdi.newBuilderForType.mergeFrom(message.getMessage).build))
          } catch {
            case ex: InvalidProtocolBufferException => request.responseCallback(Left(ex))
          }
        } else {
          val errorMsg = if (message.hasErrorMessage()) message.getErrorMessage else "<null>"
          request.responseCallback(Left(new RemoteException(message.getMessageName, message.getErrorMessage)))
        }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.info(e.getCause, "Caught exception in network layer")
}
