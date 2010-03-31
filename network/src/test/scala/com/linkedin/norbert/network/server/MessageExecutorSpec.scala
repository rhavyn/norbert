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
package com.linkedin.norbert.network.server

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.google.protobuf.Message
import com.linkedin.norbert.network.InvalidMessageException
import org.specs.util.WaitFor
import com.linkedin.norbert.protos.NorbertExampleProtos

class MessageExecutorSpec extends SpecificationWithJUnit with Mockito with WaitFor {
  val messageHandlerRegistry = mock[MessageHandlerRegistry]
  val messageExecutor = new ThreadPoolMessageExecutor(messageHandlerRegistry, 1, 1, 1)
  val message = NorbertExampleProtos.Ping.getDefaultInstance
  var handlerCalled = false
  var either: Either[Exception, Message] = null
  def handler(e: Either[Exception, Message]) {
    handlerCalled = true
    either = e
  }

  "MessageExecutor" should {
    "find the handler associated with the specified message" in {
      messageHandlerRegistry.handlerFor(message) returns returnHandler _

      messageExecutor.executeMessage(message, either => null)

      waitFor(1.ms)

      messageHandlerRegistry.handlerFor(message) was called
    }

    "execute the handler associated with the specified message" in {
      var wasCalled = false
      def h(message: Message): Message = {
        wasCalled = true
        message
      }
      messageHandlerRegistry.handlerFor(message) returns h _

      messageExecutor.executeMessage(message, either => null)

      wasCalled must eventually(beTrue)
    }

    "execute the responseHandler with Right(message) if the handler returns a valid message" in {
      messageHandlerRegistry.validResponseFor(message, message) returns true
      messageHandlerRegistry.handlerFor(message) returns returnHandler _

      messageExecutor.executeMessage(message, handler _)

      handlerCalled must eventually(beTrue)
      either.isRight must beTrue
      either.right.get must be(message)
    }

    "not execute the responseHandler if the handler returns null" in {
      messageHandlerRegistry.validResponseFor(message, null) returns true
      messageHandlerRegistry.handlerFor(message) returns nullHandler _

      messageExecutor.executeMessage(message, handler _)

      handlerCalled must eventually(beFalse)
    }

    "execute the responseHandler with Left(ex) if the handler throws an exception" in {
      messageHandlerRegistry.handlerFor(message) returns throwsHandler _

      messageExecutor.executeMessage(message, handler _)

      handlerCalled must eventually(beTrue)
      either.isLeft must beTrue
    }

    "not execute the responseHandler if the message is not registered" in {
      messageHandlerRegistry.handlerFor(message) throws new InvalidMessageException("")

      messageExecutor.executeMessage(message, handler _)

      waitFor(5.ms)

      handlerCalled must beFalse
    }

    "execute the responseHandler with Left(InvalidMessageException) if the response message is of the wrong type" in {
      messageHandlerRegistry.validResponseFor(message, message) returns false
      messageHandlerRegistry.handlerFor(message) returns returnHandler _

      messageExecutor.executeMessage(message, handler _)

      handlerCalled must eventually(beTrue)
      either.isLeft must beTrue
      either.left.get must haveClass[InvalidMessageException]
    }

    "execute the responseHandler with Left(InvalidMessageException) if the response message is null and should not be" in {
      messageHandlerRegistry.validResponseFor(message, null) returns false
      messageHandlerRegistry.handlerFor(message) returns nullHandler _

      messageExecutor.executeMessage(message, handler _)

      handlerCalled must eventually(beTrue)
      either.isLeft must beTrue
      either.left.get must haveClass[InvalidMessageException]
    }
  }

  def returnHandler(message: Message): Message = message
  def throwsHandler(message: Message): Message = throw new Exception
  def nullHandler(message: Message): Message = null
}
