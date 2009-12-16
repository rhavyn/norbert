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

import java.util.concurrent.TimeUnit
import org.specs.SpecificationWithJUnit
import com.google.protobuf.Message
import org.specs.mock.Mockito

class RequestSpec extends SpecificationWithJUnit with Mockito {
  "Request ResponseIterator implementation" should {
    "when calling hasNext" in {
      "return true if not all responses have been processed" in {
        val request = Request(mock[Message], 2)
        val iter = request.responseIterator

        iter.hasNext must beTrue
        request.offerResponse(Right(mock[Message]))
        iter.hasNext must beTrue
        request.offerResponse(Right(mock[Message]))
        iter.hasNext must beTrue

        iter.next
        iter.hasNext must beTrue
      }

      "return true if an attempt to process a response was attempted but no responses were available" in {
        val request = Request(mock[Message], 1)
        val iter = request.responseIterator
        iter.next(1, TimeUnit.MILLISECONDS)
        iter.hasNext must beTrue
      }

      "return false if all responses have been processed" in {
        val request = Request(mock[Message], 2)
        val iter = request.responseIterator

        iter.hasNext must beTrue
        request.offerResponse(Right(mock[Message]))
        iter.next
        iter.hasNext must beTrue
        request.offerResponse(Right(mock[Message]))
        iter.next
        iter.hasNext must beFalse
      }
    }

    "when calling next" in {
      "timeout and return None if no messages are available" in {
        Request(mock[Message], 1).responseIterator.next(1, TimeUnit.MILLISECONDS) must beNone
      }

      "return Some[Right[Messsage]] if a message is available" in {
        val request = Request(mock[Message], 1)
        val message = mock[Message]
        request.offerResponse(Right(message))
        request.responseIterator.next must beSome[Either[Throwable, Message]].which { either =>
          either.isRight must beTrue
          either.right.get must be_==(message)
        }
      }

      "return Some[Left[Throwable]] if a throwable is available" in {
        val request = Request(mock[Message], 1)
        val ex = new Exception
        request.offerResponse(Left(ex))
        request.responseIterator.next must beSome[Either[Throwable, Message]].which { either =>
          either.isLeft must beTrue
          either.left.get must be_==(ex)
        }
      }
    }
  }
}
