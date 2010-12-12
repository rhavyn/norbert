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
package server

import org.specs.Specification
import com.google.protobuf.Message
import org.specs.mock.Mockito
import protos.NorbertExampleProtos

class MessageHandlerRegistrySpec extends Specification with Mockito {
  val messageHandlerRegistry = new MessageHandlerRegistry
  val proto = NorbertExampleProtos.Ping.newBuilder.setTimestamp(System.currentTimeMillis).build
  var handled: Message = _
  val handler = (message: Message) => {
    handled = message
    message
  }

  "MessageHandlerRegistry" should {
    "throw a NullPointerException if the request message or handler is null" in {
      messageHandlerRegistry.registerHandler(null, null, handler) must throwA[NullPointerException]
      messageHandlerRegistry.registerHandler(proto, null, null) must throwA[NullPointerException]
      messageHandlerRegistry.registerHandler(proto, null, handler)
    }

    "return the handler for the specified request message" in {
      messageHandlerRegistry.registerHandler(proto, proto, handler)
      val h = messageHandlerRegistry.handlerFor(proto)
      h(proto) must be_==(proto)
      handled must be_==(proto)
    }

    "throw an InvalidMessageException if no handler is registered" in {
      messageHandlerRegistry.handlerFor(proto) must throwA[InvalidMessageException]
    }

    "return true if the provided response is a valid response for the given request" in {
      messageHandlerRegistry.registerHandler(proto, proto, handler)
      messageHandlerRegistry.validResponseFor(proto, NorbertExampleProtos.Ping.newBuilder.setTimestamp(System.currentTimeMillis).build) must beTrue
    }

    "return false if the provided response is not a valid response for the given request" in {
      messageHandlerRegistry.registerHandler(proto, proto, handler)
      messageHandlerRegistry.validResponseFor(proto, mock[Message]) must beFalse
    }

    "correctly handles null in validResponseFor" in {
      messageHandlerRegistry.registerHandler(proto, null, handler)
      messageHandlerRegistry.validResponseFor(proto, null) must beTrue

      messageHandlerRegistry.registerHandler(proto, proto, handler)
      messageHandlerRegistry.validResponseFor(proto, null) must beFalse
    }
  }
}
