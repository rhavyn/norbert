package com.linkedin.norbert.network

import com.linkedin.norbert.protos.NorbertExampleProtos


object Ping {
  implicit case object PingSerializer extends Serializer[Ping, Ping] {
    def nameOfRequestMessage = "ping"

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

case class Ping(timestamp: Long)
