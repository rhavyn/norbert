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
    public String requestName() {
      return "ping";
    }

    public String responseName() {
      return "pong";
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

