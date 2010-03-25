package com.linkedin.norbert.network.server

import com.google.protobuf.Message
import com.linkedin.norbert.network.InvalidMessageException

trait MessageHandlerRegistryComponent {
  val messageHandlerRegistry: MessageHandlerRegistry

  class MessageHandlerRegistry {
    private var handlerMap = Map[String, (Message, Message, (Message) => Message)]()

    def registerHandler(requestMessage: Message, responseMessage: Message, handler: (Message) => Message) {
      if (requestMessage == null || handler == null) throw new NullPointerException
      val response = if (responseMessage == null) null else responseMessage.getDefaultInstanceForType

      handlerMap += (requestMessage.getDescriptorForType.getFullName -> (requestMessage, responseMessage, handler))
    }
  }
}
