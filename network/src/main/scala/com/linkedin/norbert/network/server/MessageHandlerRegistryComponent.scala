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

import com.google.protobuf.Message
import com.linkedin.norbert.network.InvalidMessageException

trait MessageHandlerRegistryComponent {
  val messageHandlerRegistry: MessageHandlerRegistry
}

class MessageHandlerRegistry {
  private var handlerMap = Map[String, (Message, Message, (Message) => Message)]()

  def registerHandler(requestMessage: Message, responseMessage: Message, handler: (Message) => Message) {
    if (requestMessage == null || handler == null) throw new NullPointerException
    val response = if (responseMessage == null) null else responseMessage.getDefaultInstanceForType

    handlerMap += (requestMessage.getDescriptorForType.getFullName -> (requestMessage, responseMessage, handler))
  }

  @throws(classOf[InvalidMessageException])
  def handlerFor(requestMessage: Message): (Message) => Message = {
    if (requestMessage == null) throw new NullPointerException

    getHandlerTuple(requestMessage)._3
  }

  def requestMessageDefaultInstanceFor(name: String): Message = {
    handlerMap.get(name).getOrElse(throw new InvalidMessageException("No such message of type %s registered".format(name)))._1
  }

  def validResponseFor(requestMessage: Message, responseMessage: Message): Boolean = {
    if (requestMessage == null) throw new NullPointerException
    val (_, r, _) = getHandlerTuple(requestMessage)

    if (r == null && responseMessage == null) true
    else if (r != null && responseMessage == null) false
    else responseMessage.getClass == r.getClass
  }

  def getHandlerTuple(requestMessage: Message) = {
    val name = requestMessage.getDescriptorForType.getFullName
    handlerMap.get(name).getOrElse(throw new InvalidMessageException("No such message of type %s registered".format(name)))
  }
}
