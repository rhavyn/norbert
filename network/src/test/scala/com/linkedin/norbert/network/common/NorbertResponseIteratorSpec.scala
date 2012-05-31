package com.linkedin.norbert
package network
package common

import org.specs.Specification
import org.specs.mock.Mockito
import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}

class NorbertResponseIteratorSpec extends Specification with Mockito with SampleMessage {
  val responseQueue = new ResponseQueue[Ping]
  val it = new NorbertResponseIterator[Ping](2, responseQueue)

  "NorbertResponseIterator" should {
//    "return true for next until all responses have been consumed" in {
//      it.hasNext must beTrue
//
//      responseQueue += (Right(new Ping))
//      responseQueue += (Right(new Ping))
//      it.next must notBeNull
//      it.hasNext must beTrue
//
//      it.next must notBeNull
//      it.hasNext must beFalse
//    }
//
//    "return true for nextAvailable if any responses are available" in {
//      it.nextAvailable must beFalse
//      responseQueue += (Right(new Ping))
//      it.nextAvailable must beTrue
//    }
//
//    "throw a TimeoutException if no response is available" in {
//      it.next(1, TimeUnit.MILLISECONDS) must throwA[TimeoutException]
//    }
//
//    "throw an ExecutionException for an error" in {
//      val ex = new Exception
//      responseQueue += (Left(ex))
//      it.next must throwA[ExecutionException]
//    }

    "Handle exceptions at the end of the stream" in {
      val responseQueue = new ResponseQueue[Int]
      responseQueue += Right(0)
      responseQueue += Right(1)

      val norbertResponseIterator = new NorbertResponseIterator[Int](3, responseQueue)
      val timeoutIterator = new TimeoutIterator(norbertResponseIterator, 10L)
      val exceptionIterator = new ExceptionIterator(timeoutIterator)

      val partialIterator = new PartialIterator(exceptionIterator)

      partialIterator.hasNext mustBe true
      partialIterator.next mustBe 0

      partialIterator.hasNext mustBe true
      partialIterator.next mustBe 1

      partialIterator.hasNext mustBe false
    }

    "Handle exceptions in the middle of the stream" in {
      val responseQueue = new ResponseQueue[Int]
      responseQueue += Right(0)
      responseQueue += Left(new TimeoutException)
      responseQueue += Right(1)

      val norbertResponseIterator = new NorbertResponseIterator[Int](3, responseQueue)
      val timeoutIterator = new TimeoutIterator(norbertResponseIterator, 100L)
      val exceptionIterator = new ExceptionIterator(timeoutIterator)

      val partialIterator = new PartialIterator(exceptionIterator)

      partialIterator.hasNext mustBe true
      partialIterator.next mustBe 0

      partialIterator.hasNext mustBe true
      partialIterator.next mustBe 1

      partialIterator.hasNext mustBe false
    }
  }
}