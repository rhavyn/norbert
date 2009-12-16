/*
 * Copyright 2009 LinkedIn, Inc
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

import java.util.UUID
import com.google.protobuf.Message
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.linkedin.norbert.protos.NorbertProtos

class MessageHandlerComponentSpec extends SpecificationWithJUnit with Mockito with MessageHandlerComponent
        with MessageRegistryComponent {
  val messageRegistry = mock[MessageRegistry]
  val messageHandler = new MessageHandler
  val ping = NorbertProtos.Ping.getDefaultInstance

  "MessageHandler" should {
    "given a message, call the handler and return Some containing the response message" in {
      messageRegistry.defaultInstanceAndHandlerForClassName(ping.getClass.getName) returns Some((ping, somePingHandler _))
      val norbertMessage = makeNorbertMessage
      messageHandler.handleMessage(norbertMessage) must beSome[NorbertProtos.NorbertMessage].which { nm =>
        nm.getRequestIdMsb must be_==(norbertMessage.getRequestIdMsb)
        nm.getRequestIdLsb must be_==(norbertMessage.getRequestIdLsb)
        nm.getMessageName must be_==(ping.getClass.getName)
        val p = ping.toBuilder.mergeFrom(nm.getMessage).build
        p.getTimestamp must be_==(2)
      }
    }

    "given a message, call the handler and return None" in {
      messageRegistry.defaultInstanceAndHandlerForClassName(ping.getClass.getName) returns Some((ping, nonePingHandler _))
      val norbertMessage = makeNorbertMessage
      messageHandler.handleMessage(norbertMessage) must beNone
    }

    "given a message, call a handler that throws an exception and return an error message" in {
      messageRegistry.defaultInstanceAndHandlerForClassName(ping.getClass.getName) returns Some((ping, throwsPingHandler _))
      val norbertMessage = makeNorbertMessage
      messageHandler.handleMessage(norbertMessage) must beSome[NorbertProtos.NorbertMessage].which { nm =>
        nm.getRequestIdMsb must be_==(norbertMessage.getRequestIdMsb)
        nm.getRequestIdLsb must be_==(norbertMessage.getRequestIdLsb)
        nm.getStatus must be_==(NorbertProtos.NorbertMessage.Status.ERROR)
        nm.getMessageName must be_==("java.lang.Exception")
        nm.getErrorMessage must be_==("Foo")
      }
    }

    "given a message without an associated handler, return an error response" in {
      messageRegistry.defaultInstanceAndHandlerForClassName(ping.getClass.getName) returns None
      val norbertMessage = makeNorbertMessage
      messageHandler.handleMessage(norbertMessage) must beSome[NorbertProtos.NorbertMessage].which { nm =>
        nm.getRequestIdMsb must be_==(norbertMessage.getRequestIdMsb)
        nm.getRequestIdLsb must be_==(norbertMessage.getRequestIdLsb)
        nm.getStatus must be_==(NorbertProtos.NorbertMessage.Status.ERROR)
      }
    }

    "given an error message, return an error response" in {
      val uuid = UUID.randomUUID
      val builder = NorbertProtos.NorbertMessage.newBuilder
      builder.setRequestIdMsb(uuid.getMostSignificantBits)
      builder.setRequestIdLsb(uuid.getLeastSignificantBits)
      builder.setStatus(NorbertProtos.NorbertMessage.Status.ERROR)
      builder.setMessageName(ping.getClass.getName)
      builder.setMessage(makePing(1).toByteString)
      val norbertMessage = builder.build

      messageRegistry.defaultInstanceAndHandlerForClassName(ping.getClass.getName) returns None
      messageHandler.handleMessage(norbertMessage) must beSome[NorbertProtos.NorbertMessage].which { nm =>
        nm.getRequestIdMsb must be_==(norbertMessage.getRequestIdMsb)
        nm.getRequestIdLsb must be_==(norbertMessage.getRequestIdLsb)
        nm.getStatus must be_==(NorbertProtos.NorbertMessage.Status.ERROR)
      }
    }
  }

  private def makeNorbertMessage = {
    val uuid = UUID.randomUUID
    val builder = NorbertProtos.NorbertMessage.newBuilder
    builder.setRequestIdMsb(uuid.getMostSignificantBits)
    builder.setRequestIdLsb(uuid.getLeastSignificantBits)
    builder.setMessageName(ping.getClass.getName)
    builder.setMessage(makePing(1).toByteString)
    builder.build
  }

  private def makePing(timestamp: Long) = NorbertProtos.Ping.newBuilder.setTimestamp(timestamp).build

  private def somePingHandler(message: Message): Option[Message] = Some(makePing(2))

  private def nonePingHandler(message: Message): Option[Message] = None

  private def throwsPingHandler(message: Message): Option[Message] = throw new Exception("Foo")
}
