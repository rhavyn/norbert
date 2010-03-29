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
package com.linkedin.norbert.network.common

import org.specs.SpecificationWithJUnit
import java.lang.NullPointerException
import org.specs.mock.Mockito
import com.linkedin.norbert.network.InvalidMessageException
import com.google.protobuf.Message
import com.linkedin.norbert.protos.NorbertProtos

class MessageRegistrySpec extends SpecificationWithJUnit with Mockito {
  val messageRegistry = new MessageRegistry

  val proto = NorbertProtos.Ping.newBuilder.setTimestamp(System.currentTimeMillis).build

  "MessageRegistry" should {
    "throw a NullPointerException if requestMessage is null" in {
      messageRegistry.registerMessage(null, null) must throwA[NullPointerException]
    }

    "throw an InvalidMessageExceptio if the requestMessage isn't registered" in {
      messageRegistry.hasResponse(proto) must throwA[InvalidMessageException]
      messageRegistry.responseMessageDefaultInstanceFor(proto) must throwA[InvalidMessageException]
    }

    "contains returns true if the specified request message has been registered" in {
      val response = mock[Message]

      messageRegistry.contains(proto) must beFalse
      messageRegistry.registerMessage(proto, proto)
      messageRegistry.contains(proto) must beTrue
    }

    "return true for hasResponse if the responseMessage is not null" in {
      messageRegistry.registerMessage(proto, null)
      messageRegistry.hasResponse(proto) must beFalse

      messageRegistry.registerMessage(proto, proto)
      messageRegistry.hasResponse(proto) must beTrue
    }

    "return true if the response message is of the correct type" in {
      val name = "norbert.PingResponse"
      messageRegistry.registerMessage(proto, null)
      messageRegistry.validResponseFor(proto, name) must beFalse

      messageRegistry.registerMessage(proto, proto)
      messageRegistry.validResponseFor(proto, name) must beFalse

      messageRegistry.validResponseFor(proto, proto.getDescriptorForType.getFullName) must beTrue
    }
  }
}
