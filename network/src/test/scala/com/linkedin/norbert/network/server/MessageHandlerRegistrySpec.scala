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
import org.specs.mock.Mockito
import protos.NorbertExampleProtos
import common.SampleMessage

class MessageHandlerRegistrySpec extends Specification with Mockito with SampleMessage {
  val messageHandlerRegistry = new MessageHandlerRegistry

  var handled: Ping = _
  val handler = (ping: Ping) => {
    handled = ping
    ping
  }


  "MessageHandlerRegistry" should {
    "return the handler for the specified request message" in {
      messageHandlerRegistry.registerHandler(handler)

      val h = messageHandlerRegistry.handlerFor[Ping, Ping](serializer.requestName)

      h(request) must be_==(request)
      handled must be_==(request)
    }

    "throw an InvalidMessageException if no handler is registered" in {
      messageHandlerRegistry.handlerFor(serializer.requestName) must throwA[InvalidMessageException]
    }

    "return true if the provided response is a valid response for the given request" in {
      messageHandlerRegistry.registerHandler(handler)
//      messageHandlerRegistry.validResponseFor(proto, NorbertExampleProtos.Ping.newBuilder.setTimestamp(System.currentTimeMillis).build) must beTrue
    }

    "return false if the provided response is not a valid response for the given request" in {
      messageHandlerRegistry.registerHandler(handler)
//      messageHandlerRegistry.validResponseFor(proto, mock[Message]) must beFalse
    }

//    "correctly handles null in validResponseFor" in {
//      messageHandlerRegistry.registerHandler(proto, null, handler)
//      messageHandlerRegistry.validResponseFor(proto, null) must beTrue
//
//      messageHandlerRegistry.registerHandler(proto, proto, handler)
//      messageHandlerRegistry.validResponseFor(proto, null) must beFalse
//    }
  }
}
