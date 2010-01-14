package com.linkedin.norbert.network

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.google.protobuf.Message
import com.linkedin.norbert.protos.NorbertProtos
import org.specs.util.WaitFor

class MessageExecutorComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with MessageExecutorComponent with MessageRegistryComponent {
  val messageRegistry = mock[MessageRegistry]
  val messageExecutor = new MessageExecutor(1, 1, 1)

  "MessageExecutor" should {
    val message = NorbertProtos.Ping.getDefaultInstance
    val messageClassName = message.getClass.getName

    "find the handler associated with the specified message" in {
      messageRegistry.handlerForClassName(messageClassName) returns Some(someHandler _)

      messageExecutor.executeMessage(message, either => null)

      waitFor(1.ms)
      
      messageRegistry.handlerForClassName(messageClassName) was called
    }

    "execute the handler associated with the specified message" in {
      var called = false
      def handler(message: Message): Option[Message] = {
        called = true
        Some(message)
      }
      messageRegistry.handlerForClassName(messageClassName) returns Some(handler _)

      messageExecutor.executeMessage(message, either => null)

      waitFor(1.ms)

      called must beTrue
    }

    "execute the responseHandler with Right(message) if the handler returns Some(message)" in {
      var called = false
      var either: Either[Exception, Message] = null
      def handler(e: Either[Exception, Message]) {
        called = true
        either = e
      }
      messageRegistry.handlerForClassName(messageClassName) returns Some(someHandler _)

      messageExecutor.executeMessage(message, handler _)

      waitFor(1.ms)

      called must beTrue
      either.isRight must beTrue
      either.right.get must be(message)
    }

    "not execute the responseHandler if the handler returns none" in {
      var called = false
      def handler(either: Either[Exception, Message]) {
        called = true
      }
      messageRegistry.handlerForClassName(messageClassName) returns Some(noneHandler _)

      messageExecutor.executeMessage(message, handler _)

      waitFor(1.ms)

      called must beFalse      
    }

    "execute the responseHandler with Left(ex) if the handler throws an exception" in {
      var called = false
      var either: Either[Exception, Message] = null
      def handler(e: Either[Exception, Message]) {
        called = true
        either = e
      }
      messageRegistry.handlerForClassName(messageClassName) returns Some(throwsHandler _)

      messageExecutor.executeMessage(message, handler _)

      waitFor(1.ms)

      called must beTrue
      either.isLeft must beTrue
    }

    "execute the responseHandler with Left(InvalidMessageException) if the message is not registered" in {
      var called = false
      var either: Either[Exception, Message] = null
      def handler(e: Either[Exception, Message]) {
        called = true
        either = e
      }
      messageRegistry.handlerForClassName(messageClassName) returns None

      messageExecutor.executeMessage(message, handler _)

      waitFor(1.ms)

      called must beTrue
      either.isLeft must beTrue
      either.left.get must haveClass[InvalidMessageException]
    }
  }

  def someHandler(message: Message): Option[Message] = Some(message)
  def throwsHandler(message: Message): Option[Message] = throw new Exception
  def noneHandler(message: Message): Option[Message] = None
}
