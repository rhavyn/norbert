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
import server.{MessageExecutor, MessageHandlerRegistry}
import protos.NorbertProtos
import logging.Logging
import java.util.UUID
import jmx.JMX.MBean
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}
import jmx.{FinishedRequestTimeTracker, JMX}
import java.lang.String
import com.google.protobuf.{ByteString}
import common.NetworkStatisticsActor
import util.SystemClock

case class RequestContext(requestId: UUID, receivedAt: Long = System.currentTimeMillis)

@ChannelPipelineCoverage("all")
class RequestContextDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, channel: Channel, msg: Any) = {
    val norbertMessage = msg.asInstanceOf[NorbertProtos.NorbertMessage]
    val requestId = new UUID(norbertMessage.getRequestIdMsb, norbertMessage.getRequestIdLsb)

    if (norbertMessage.getStatus != NorbertProtos.NorbertMessage.Status.OK) {
      val ex = new InvalidMessageException("Invalid request, message has status set to ERROR")
      Channels.write(ctx, Channels.future(channel), ResponseHelper.errorResponse(requestId, ex))
      throw ex
    }

    (RequestContext(requestId), norbertMessage)
  }
}

@ChannelPipelineCoverage("all")
class RequestContextEncoder extends OneToOneEncoder with Logging {

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: Any) = {
    val (context, norbertMessage) = msg.asInstanceOf[(RequestContext, NorbertProtos.NorbertMessage)]


    norbertMessage
  }

}

@ChannelPipelineCoverage("all")
class ServerChannelHandler(serviceName: String, channelGroup: ChannelGroup, messageHandlerRegistry: MessageHandlerRegistry, messageExecutor: MessageExecutor, requestStatisticsWindow: Long) extends SimpleChannelHandler with Logging {
  private val statsActor = new NetworkStatisticsActor[Int, UUID](SystemClock, requestStatisticsWindow)
  statsActor.start

  private val jmxHandle = JMX.register(new MBean(classOf[NetworkServerStatisticsMBean], "service=%s".format(serviceName)) with NetworkServerStatisticsMBean {
    import statsActor.Stats._

    def getRequestsPerSecond = statsActor !? GetRequestsPerSecond match {
      case RequestsPerSecond(rps) => rps
    }

    def getAverageRequestProcessingTime = statsActor !? GetProcessingStatistics match {
      case ProcessingStatistics(map) => average(map){_.completedTime}{_.completedSize}
    }
  })

  def shutdown: Unit = jmxHandle.foreach { JMX.unregister(_) }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val channel = e.getChannel
    log.trace("channelOpen: " + channel)
    channelGroup.add(channel)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val (context, norbertMessage) = e.getMessage.asInstanceOf[(RequestContext, NorbertProtos.NorbertMessage)]
    val channel = e.getChannel

    val messageName = norbertMessage.getMessageName
    val requestBytes = norbertMessage.getMessage.toByteArray

    statsActor ! statsActor.Stats.BeginRequest(0, context.requestId)

    val (handler, is, os) = try {
      val handler: Any => Any = messageHandlerRegistry.handlerFor(messageName)
      val is: InputSerializer[Any, Any] = messageHandlerRegistry.inputSerializerFor(messageName)
      val os: OutputSerializer[Any, Any] = messageHandlerRegistry.outputSerializerFor(messageName)

      (handler, is, os)
    } catch {
      case ex: InvalidMessageException =>
        Channels.write(ctx, Channels.future(channel), (context, ResponseHelper.errorResponse(context.requestId, ex)))
        throw ex
    }

    val request = is.requestFromBytes(requestBytes)

    try {
      messageExecutor.executeMessage(request, (either: Either[Exception, Any]) => {
        responseHandler(context, e.getChannel, either)(is, os)
      })(is)
    }
    catch {
      case ex: HeavyLoadException =>
        Channels.write(ctx, Channels.future(channel), (context, ResponseHelper.errorResponse(context.requestId, ex, NorbertProtos.NorbertMessage.Status.HEAVYLOAD)))
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.info(e.getCause, "Caught exception in channel: %s".format(e.getChannel))

  def responseHandler[RequestMsg, ResponseMsg](context: RequestContext, channel: Channel, either: Either[Exception, ResponseMsg])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    val response = either match {
      case Left(ex) => ResponseHelper.errorResponse(context.requestId, ex)
      case Right(responseMsg) =>
        ResponseHelper.responseBuilder(context.requestId)
        .setMessageName(is.nameOfRequestMessage)
        .setMessage(ByteString.copyFrom(os.responseToBytes(responseMsg)))
        .build
    }

    log.debug("Sending response: %s".format(response))

    channel.write((context, response))
  }
}

private[netty] object ResponseHelper {
  def responseBuilder(requestId: UUID) = {
    NorbertProtos.NorbertMessage.newBuilder.setRequestIdMsb(requestId.getMostSignificantBits).setRequestIdLsb(requestId.getLeastSignificantBits)
  }

  def errorResponse(requestId: UUID, ex: Exception, status :NorbertProtos.NorbertMessage.Status = NorbertProtos.NorbertMessage.Status.ERROR) = {
    responseBuilder(requestId)
            .setMessageName(ex.getClass.getName)
            .setStatus(status)
            .setErrorMessage(if (ex.getMessage == null) "" else ex.getMessage)
            .build
  }


}

trait NetworkServerStatisticsMBean {
  def getRequestsPerSecond: Int
  def getAverageRequestProcessingTime: Double
}
