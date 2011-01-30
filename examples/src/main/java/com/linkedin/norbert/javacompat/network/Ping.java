package com.linkedin.norbert.javacompat.network;

import com.google.protobuf.InvalidProtocolBufferException;
import com.linkedin.norbert.network.Serializer;
import com.linkedin.norbert.protos.NorbertExampleProtos;

class Ping {
  public final long timestamp;

  public Ping(long timestamp) {
    this.timestamp = timestamp;
  }
}

class PingSerializer implements Serializer<Ping, Ping> {
    public String nameOfRequestMessage() {
      return "ping";
    }

    public byte[] requestToBytes(Ping message) {
      return NorbertExampleProtos.Ping.newBuilder().setTimestamp(message.timestamp).build().toByteArray();
    }

    public Ping requestFromBytes(byte[] bytes) {
      try {
        return new Ping(NorbertExampleProtos.Ping.newBuilder().mergeFrom(bytes).build().getTimestamp());
      } catch (InvalidProtocolBufferException e) {
         System.out.println("Invalid protocol buffer exception " + e.getMessage());
         throw new IllegalArgumentException(e);
      }
    }

    public byte[] responseToBytes(Ping message) {
      return requestToBytes(message);
    }

    public Ping responseFromBytes(byte[] bytes) {
      return requestFromBytes(bytes);
    }
}

