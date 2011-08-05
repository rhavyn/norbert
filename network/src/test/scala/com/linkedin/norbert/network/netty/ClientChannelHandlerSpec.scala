package com.linkedin.norbert
package network
package netty

import org.specs.Specification
import org.specs.mock.Mockito
import client.ResponseHandler
import org.jboss.netty.channel._
import protos.NorbertProtos.NorbertMessage.Status
import protos.{NorbertExampleProtos, NorbertProtos}
import cluster.Node
import com.linkedin.norbert.network.Serializer
import netty.Ping.PingSerializer
import java.net.{SocketAddress}

/**
 * Test to cover association of RequestAccess with remote exception
 */
class ClientChannelHandlerSpec extends Specification with Mockito {

  val responseHandler = mock[ResponseHandler]
  val clientChannelHandler = new ClientChannelHandler(clientName = Some("booClient"),
    serviceName = "booService",
    staleRequestTimeoutMins = 3000,
    staleRequestCleanupFrequencyMins = 3000,
    requestStatisticsWindow = 3000L,
    outlierMultiplier = 2,
    outlierConstant = 2,
    responseHandler = responseHandler,
    avoidByteStringCopy = true)

  def sendMockRequest(ctx: ChannelHandlerContext, request: Request[Ping, Ping]) {
      val writeEvent = mock[MessageEvent]
      writeEvent.getChannel returns ctx.getChannel
      val channelFuture = mock[ChannelFuture]
      writeEvent.getFuture returns channelFuture
      writeEvent.getMessage returns request
      clientChannelHandler.writeRequested(ctx, writeEvent)
  }

  "ClientChannelHandler" should {
    "throw exception with RequestAccess when server response is HEAVYLOAD" in {
      val channel = mock[Channel]
      channel.getRemoteAddress returns mock[SocketAddress]
      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel
      val request = Request[Ping, Ping](Ping(System.currentTimeMillis), Node(1, "localhost:1234", true), PingSerializer, PingSerializer, { e => e })
      sendMockRequest(ctx, request)

      val readEvent = mock[MessageEvent]
      val norbertMessage = NorbertProtos.NorbertMessage.newBuilder().setStatus(Status.HEAVYLOAD)
        .setRequestIdLsb(request.id.getLeastSignificantBits)
        .setRequestIdMsb(request.id.getMostSignificantBits)
        .setMessageName("Boo")
        .build
      readEvent.getMessage returns norbertMessage
      clientChannelHandler.messageReceived(ctx, readEvent)
      there was one(responseHandler).onFailure(any[Request[_,_]], any[Throwable with RequestAccess[Request[_, _]]])
    }

    "throw exception with RequestAccess when server response is ERROR" in {
      val channel = mock[Channel]
      channel.getRemoteAddress returns mock[SocketAddress]
      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel
      val request = Request[Ping, Ping](Ping(System.currentTimeMillis), Node(1, "localhost:1234", true), PingSerializer, PingSerializer, { e => e })
      sendMockRequest(ctx, request)

      val norbertMessage = NorbertProtos.NorbertMessage.newBuilder().setStatus(Status.ERROR)
        .setRequestIdLsb(request.id.getLeastSignificantBits)
        .setRequestIdMsb(request.id.getMostSignificantBits)
        .setMessageName("Boo")
        .setErrorMessage("BooBoo")
        .build
      val readEvent = mock[MessageEvent]
      readEvent.getMessage returns norbertMessage
      clientChannelHandler.messageReceived(ctx, readEvent)
      there was one(responseHandler).onFailure(any[Request[_,_]], any[Throwable with RequestAccess[Request[_, _]]])
    }
  }

}

// temp workaround to quiet writeRequested
private object Ping {
  implicit case object PingSerializer extends Serializer[Ping, Ping] {
    def requestName = "ping"
    def responseName = "pong"

    def requestToBytes(message: Ping) =
      NorbertExampleProtos.Ping.newBuilder.setTimestamp(message.timestamp).build.toByteArray

    def requestFromBytes(bytes: Array[Byte]) = {
      Ping(NorbertExampleProtos.Ping.newBuilder.mergeFrom(bytes).build.getTimestamp)
    }

    def responseToBytes(message: Ping) =
      requestToBytes(message)

    def responseFromBytes(bytes: Array[Byte]) =
      requestFromBytes(bytes)
  }
}

private case class Ping(timestamp: Long)
