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

import org.specs.SpecificationWithJUnit
import com.google.protobuf.Message
import org.specs.mock.Mockito
import com.linkedin.norbert.protos.NorbertProtos

class MessageRegistryComponentSpec extends SpecificationWithJUnit with Mockito with MessageRegistryComponent {
  val ping = NorbertProtos.Ping.getDefaultInstance
  val messageRegistry = null

  "DefaultMessageRegistry" should {
    "if only unhandled messages are provided" in {
      val messageRegistry = new DefaultMessageRegistry(Array(NorbertProtos.Ping.getDefaultInstance))

      "for defaultInstanceForClassName" in {
        "return the correct message instance given a class name that exists" in {
          messageRegistry.defaultInstanceForClassName(ping.getClass.getName) must beSome[Message].which(_ == ping)
        }

        "return none given a class that doesn't exist" in {
          messageRegistry.defaultInstanceForClassName("Doesn't Exist") must beNone
        }
      }

      "for defaultInstanceAndHandlerForClassName" in {
        "throw UnsupportedOperationException for class name that exists" in {
          messageRegistry.defaultInstanceAndHandlerForClassName(ping.getClass.getName) must beNone
        }

        "throw UnsupportedOperationException for a class that doesn't exist" in {
          messageRegistry.defaultInstanceAndHandlerForClassName("Doesn't Exist") must beNone
        }
      }
    }

    "if only handled messages are provided" in {
      val messageRegistry = new DefaultMessageRegistry(Array((NorbertProtos.Ping.getDefaultInstance, messageHandler _)))

      "for defaultInstanceForClassName" in {
        "return the correct message instance given a class name that exists" in {
          messageRegistry.defaultInstanceForClassName(ping.getClass.getName) must beSome[Message].which(_ == ping)
        }

        "return none given a class that doesn't exist" in {
          messageRegistry.defaultInstanceForClassName("Doesn't Exist") must beNone
        }
      }

      "for defaultInstanceAndHandlerForClassName" in {
        "return the correct message instance and handler given a class name that exists" in {
          messageRegistry.defaultInstanceAndHandlerForClassName(ping.getClass.getName) must beSome[(Message, (Message) => Option[Message])].which {
            case(message, handler) =>
              message must be_==(ping)
              handler(message) must beNone
          }
        }

        "return none given a class that doesn't exist" in {
          messageRegistry.defaultInstanceAndHandlerForClassName("Doesn't Exist") must beNone
        }
      }
    }
  }

  private def messageHandler(message: Message): Option[Message] = None
}
