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
package common

import org.specs.Specification
import com.google.protobuf.Message
import org.specs.mock.Mockito
import java.util.concurrent.{TimeoutException, ExecutionException, TimeUnit}

class NorbertFutureSpec extends Specification with Mockito {
  val future = new NorbertFuture

  "NorbertFuture" should {
    "not be done when created" in {
      future.isDone must beFalse
    }

    "be done when value is set" in {
      future.offerResponse(Right(mock[Message]))
      future.isDone must beTrue
    }

    "return the value that is set" in {
      val message = mock[Message]
      future.offerResponse(Right(message))
      future.get must be(message)
      future.get(1, TimeUnit.MILLISECONDS) must be(message)
    }

    "throw a TimeoutException if no response is available" in {
      future.get(1, TimeUnit.MILLISECONDS) must throwA[TimeoutException]
    }

    "throw an ExecutionExcetion for an error" in {
      val ex = new Exception
      future.offerResponse(Left(ex))
      future.get must throwA[ExecutionException]
      future.get(1, TimeUnit.MILLISECONDS) must throwA[ExecutionException]
    }
  }
}

class NorbertResponseIteratorSpec extends Specification with Mockito {
  val it = new NorbertResponseIterator(2)

  "NorbertResponseIterator" should {
    "return true for next until all responses have been consumed" in {
      it.hasNext must beTrue

      it.offerResponse(Right(mock[Message]))
      it.offerResponse(Right(mock[Message]))
      it.next must notBeNull
      it.hasNext must beTrue

      it.next must notBeNull
      it.hasNext must beFalse
    }

    "return true for nextAvailable if any responses are available" in {
      it.nextAvailable must beFalse
      it.offerResponse(Right(mock[Message]))
      it.nextAvailable must beTrue
    }

    "throw a TimeoutException if no response is available" in {
      it.next(1, TimeUnit.MILLISECONDS) must throwA[TimeoutException]
    }

    "throw an ExecutionException for an error" in {
      val ex = new Exception
      it.offerResponse(Left(ex))
      it.next must throwA[ExecutionException]
    }
  }
}
