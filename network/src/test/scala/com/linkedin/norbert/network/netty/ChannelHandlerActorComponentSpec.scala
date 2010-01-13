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

import org.specs.SpecificationWithJUnit
import com.linkedin.norbert.network.{MessageRegistryComponent, MessageHandlerComponent}
import org.specs.mock.Mockito
import org.specs.util.WaitFor
import org.jboss.netty.channel.{ChannelFuture, MessageEvent, Channel}
import org.mockito.Matchers._
import com.linkedin.norbert.protos.NorbertProtos

class ChannelHandlerActorComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor
        with ChannelHandlerActorComponent with MessageHandlerComponent with MessageRegistryComponent {
  val messageHandler = mock[MessageHandler]
  val messageRegistry = null

  "ChannelHandlerActor" should {
    "for MessageReceived" in {
      "invoke the message handler and, if there is a response message, send it to the client" in {
        val message = NorbertProtos.NorbertMessage.getDefaultInstance
        messageHandler.handleMessage(message) returns Some(message)
        val channel =  mock[Channel]
        channel.write(message) returns mock[ChannelFuture]
        val event = mock[MessageEvent]
        event.getMessage returns message

        val actor = new ChannelHandlerActor(channel)
        actor.start
        actor ! NettyMessages.MessageReceived(null, event)
        waitFor(10.ms)

        messageHandler.handleMessage(message) was called
        channel.write(message) was called
      }

      "invoke the message handler and, if there isn't a response message, do nothing" in {
        val message = NorbertProtos.NorbertMessage.getDefaultInstance
        messageHandler.handleMessage(message) returns None
        val channel =  mock[Channel]
        val event = mock[MessageEvent]
        event.getMessage returns message

        val actor = new ChannelHandlerActor(channel)
        actor.start
        actor ! NettyMessages.MessageReceived(null, event)
        waitFor(10.ms)

        messageHandler.handleMessage(message) was called
        channel.write(isA(classOf[NorbertProtos.NorbertMessage])) wasnt called
      }
    }
  }
}
