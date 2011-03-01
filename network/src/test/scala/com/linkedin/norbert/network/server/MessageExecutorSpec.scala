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
import org.specs.util.WaitFor
import common.SampleMessage

class MessageExecutorSpec extends Specification with Mockito with WaitFor with SampleMessage {
  val messageHandlerRegistry = mock[MessageHandlerRegistry]
  val messageExecutor = new ThreadPoolMessageExecutor(messageHandlerRegistry, 1, 1, 1, 100, 1000L)

  var handlerCalled = false
  var either: Either[Exception, Ping] = null

  val unregisteredSerializer = {
    val s = mock[Serializer[Ping, Ping]]
    s.requestName returns ("Foo")
    s
  }

  def handler(e: Either[Exception, Ping]) {
    handlerCalled = true
    either = e
  }

  "MessageExecutor" should {
    doAfter {
      messageExecutor.shutdown
    }

    "find the handler associated with the specified message" in {
      messageHandlerRegistry.handlerFor(request) returns returnHandler _

      messageExecutor.executeMessage(request, (either: Either[Exception, Ping]) => null)

      waitFor(10.ms)

      there was one(messageHandlerRegistry).handlerFor(request)
    }

    "execute the handler associated with the specified message" in {
      var wasCalled = false
      def h(message: Ping): Ping = {
        wasCalled = true
        message
      }
      messageHandlerRegistry.handlerFor(request) returns h _

      messageExecutor.executeMessage(request, (either: Either[Exception, Ping]) => null)

      wasCalled must eventually(beTrue)
    }

    "execute the responseHandler with Right(message) if the handler returns a valid message" in {
//      messageHandlerRegistry.validResponseFor(request, request) returns true
      messageHandlerRegistry.handlerFor(request) returns returnHandler _

      messageExecutor.executeMessage(request, handler _)

      handlerCalled must eventually(beTrue)
      either.isRight must beTrue
      either.right.get must be(request)
    }

    "not execute the responseHandler if the handler returns null" in {
//      messageHandlerRegistry.validResponseFor(request, null) returns true
      messageHandlerRegistry.handlerFor(request) returns nullHandler _

      messageExecutor.executeMessage(request, handler _)

      handlerCalled must eventually(beFalse)
    }

    "execute the responseHandler with Left(ex) if the handler throws an exception" in {
      messageHandlerRegistry.handlerFor(request) returns throwsHandler _

      messageExecutor.executeMessage(request, handler _)

      handlerCalled must eventually(beTrue)
      either.isLeft must beTrue
    }

    "not execute the responseHandler if the message is not registered" in {
      messageHandlerRegistry.handlerFor(request) throws new InvalidMessageException("")

      messageExecutor.executeMessage(request, handler _)

      waitFor(5.ms)

      handlerCalled must eventually(beTrue)
      either.isLeft must beTrue
      either.left.get must haveClass[InvalidMessageException]
    }

//    "execute the responseHandler with Left(InvalidMessageException) if the response message is of the wrong type" in {
////      messageHandlerRegistry.validResponseFor(request, request) returns false
//      messageHandlerRegistry.handlerFor(request) returns returnHandler _
//
//      messageExecutor.executeMessage(request, handler _)(unregisteredSerializer)
//
//      handlerCalled must eventually(beTrue)
//      either.isLeft must beTrue
//
//      println(either.left.get.getStackTraceString)
//      either.left.get must haveClass[InvalidMessageException]
//    }
//
//    "execute the responseHandler with Left(InvalidMessageException) if the response message is null and should not be" in {
////      messageHandlerRegistry.validResponseFor(request, null) returns false
//      messageHandlerRegistry.handlerFor(request) returns nullHandler _
//
//      messageExecutor.executeMessage(request, handler _)(unregisteredSerializer)
//
//      handlerCalled must eventually(beTrue)
//      either.isLeft must beTrue
//
//      either.left.get must haveClass[InvalidMessageException]
//    }
  }

  def returnHandler(message: Ping): Ping = message
  def throwsHandler(message: Ping): Ping = throw new Exception
  def nullHandler(message: Ping): Ping = null
}
