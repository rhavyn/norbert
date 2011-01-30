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

trait MessageHandlerRegistryComponent {
  val messageHandlerRegistry: MessageHandlerRegistry
}

private case class MessageHandlerEntry[RequestMsg, ResponseMsg]
(serializer: Serializer[RequestMsg, ResponseMsg], handler: RequestMsg => ResponseMsg)

class MessageHandlerRegistry {
  @volatile private var handlerMap =
    Map.empty[String, MessageHandlerEntry[_ <: Any, _ <: Any]]

  def registerHandler[RequestMsg, ResponseMsg](handler: RequestMsg => ResponseMsg)
                                              (implicit serializer: Serializer[RequestMsg, ResponseMsg]) {
    if(handler == null) throw new NullPointerException
    handlerMap += (serializer.nameOfRequestMessage -> MessageHandlerEntry(serializer, handler))
  }

  @throws(classOf[InvalidMessageException])
  def serializerFor[RequestMsg, ResponseMsg](messageName: String): Serializer[RequestMsg, ResponseMsg] = {
    handlerMap.get(messageName).map(_.serializer)
      .getOrElse(throw new InvalidMessageException("%s is not a registered method".format(messageName)))
      .asInstanceOf[Serializer[RequestMsg, ResponseMsg]]
  }

  @throws(classOf[InvalidMessageException])
  def handlerFor[RequestMsg, ResponseMsg](request: RequestMsg)
                                         (implicit serializer: Serializer[RequestMsg, ResponseMsg]): RequestMsg => ResponseMsg = {
    handlerFor[RequestMsg, ResponseMsg](serializer.nameOfRequestMessage)
  }

  @throws(classOf[InvalidMessageException])
  def handlerFor[RequestMsg, ResponseMsg](messageName: String): RequestMsg => ResponseMsg = {
    println("The handler iss.... " + handlerMap.get(messageName))

    handlerMap.get(messageName).map(_.handler)
      .getOrElse(throw new InvalidMessageException("%s is not a registered method".format(messageName)))
      .asInstanceOf[RequestMsg => ResponseMsg]
  }
}