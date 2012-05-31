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
package com.linkedin.norbert.network

import com.linkedin.norbert.protos.NorbertExampleProtos

object Ping {
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

case class Ping(timestamp: Long)
